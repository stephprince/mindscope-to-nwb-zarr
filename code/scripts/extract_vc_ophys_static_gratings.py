"""Cache the Visual Coding 2p ``static_gratings`` stimulus tables via the AllenSDK.

Why this exists
---------------
The upstream converter ``catalystneuro/visual-coding-to-nwb-v2`` (which generated
DANDI:000728) has a bug in ``interfaces/_static_grating_stimulus.py`` (issue #49): it
computes the blank-sweep mask with row indexing (``nan[0] & nan[1] & nan[2]``) instead of
column indexing, so the mask has length 3 and the ``zip`` that builds the TimeIntervals
truncates **every** ``static_gratings`` table to exactly 3 rows. Our Zarr conversion reads
those DANDI files, so it inherits the 3-row table.

To fix this we rebuild the table from the authoritative AllenSDK
``get_stimulus_table("static_gratings")`` (the validated, non-buggy path). AllenSDK is
incompatible with this repo's main environment (it needs pynwb 2.x / an older stack,
while the conversion pins pynwb 4.1.0), so it is run here in a **separate** environment and
its output is *cached to disk*. The Zarr conversion then reads these cached tables — no
AllenSDK in the conversion env.

This script streams each v1 Brain Observatory NWB directly from the public
``allen-brain-observatory`` S3 bucket with ``remfile`` (h5py fetches only the few small
stimulus/timestamp datasets it needs — no full ~hundreds-of-MB download), so it does not
require the AllenSDK download cache.

Environment
-----------
Run with the isolated AllenSDK interpreter, NOT ``uv run``::

    scratch/allensdk_env/Scripts/python.exe code/scripts/extract_vc_ophys_static_gratings.py

(Env built with: ``uv venv scratch/allensdk_env --python 3.11`` then
``uv pip install "allensdk @ git+https://github.com/AllenInstitute/AllenSDK.git@master" remfile``.)

Output
------
One CSV per experiment at ``data/visual-coding-ophys-static-gratings/<experiment_id>.csv``
with columns: ``start_time, stop_time, orientation_in_degrees,
spatial_frequency_in_cycles_per_degree, phase, is_blank_sweep``. Resumable: existing files
are skipped unless ``--overwrite``.
"""
import argparse
import json
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
import remfile
from allensdk.core.brain_observatory_nwb_data_set import BrainObservatoryNwbDataSet

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent.parent
OPHYS_JSON = REPO_ROOT / "data" / "allen-brain-observatory" / "visual-coding-2p" / "ophys_experiments.json"
CACHE_DIR = REPO_ROOT / "data" / "visual-coding-ophys-static-gratings"

# Only three_session_B contains the static_gratings stimulus.
STIM_B = "three_session_B"

# Public S3 (HTTPS) location of the v1 Brain Observatory ophys NWB files.
V1_NWB_URL = "https://allen-brain-observatory.s3.amazonaws.com/visual-coding-2p/ophys_experiment_data/{experiment_id}.nwb"

# Presentation duration was hard-coded (not explicitly synchronized) in the original data
# and in the upstream converter; reused here so stop_time matches the upstream convention.
STATIC_GRATING_DURATION_S = 0.25

# Parameter columns whose simultaneous NaN marks a blank (mean-luminance gray) sweep.
PARAM_COLS = ("orientation", "spatial_frequency", "phase")


class RemoteBrainObservatoryNwbDataSet(BrainObservatoryNwbDataSet):
    """A BrainObservatoryNwbDataSet backed by a streamed (file-like) NWB, not a local path.

    The base ``__init__`` calls ``os.path.exists(self.nwb_file)`` purely to read an optional
    pipeline-version warning; every real reader uses ``h5py.File(self.nwb_file)``, which
    accepts a file-like object. Bypassing ``__init__`` lets us hand it a ``remfile.File``.
    """

    def __init__(self, file_like):
        self.nwb_file = file_like
        self.pipeline_version = None
        self._stimulus_search = None


def build_static_gratings_table(experiment_id: int) -> pd.DataFrame:
    """Stream one v1 NWB and return the full static_gratings table.

    Columns: start_time, stop_time, orientation_in_degrees,
    spatial_frequency_in_cycles_per_degree, phase, is_blank_sweep.
    """
    url = V1_NWB_URL.format(experiment_id=experiment_id)
    remote = remfile.File(url)
    dataset = RemoteBrainObservatoryNwbDataSet(remote)

    # AllenSDK's validated parser: columns orientation, spatial_frequency, phase, start, end
    # (start/end are 2p frame indices). Blank sweeps have NaN in the three parameter columns.
    table = dataset.get_stimulus_table("static_gratings")
    # Per-frame acquisition timestamps in seconds; map the start frame index to a start time.
    timestamps = dataset.get_fluorescence_timestamps()

    if len(table) <= 3:
        # The whole point is to avoid the 3-row truncation; a real StimB session has thousands.
        raise RuntimeError(
            f"static_gratings table for {experiment_id} has only {len(table)} rows "
            f"(expected thousands); refusing to cache a suspicious table."
        )

    start_frames = table["start"].to_numpy(dtype=int)
    if start_frames.max() >= len(timestamps):
        raise RuntimeError(
            f"start frame {start_frames.max()} out of range for {len(timestamps)} timestamps "
            f"(experiment {experiment_id})."
        )
    start_time = timestamps[start_frames]
    stop_time = start_time + STATIC_GRATING_DURATION_S

    is_blank_sweep = np.isnan(table[list(PARAM_COLS)].to_numpy(dtype=float)).all(axis=1)

    return pd.DataFrame(
        {
            "start_time": start_time,
            "stop_time": stop_time,
            "orientation_in_degrees": table["orientation"].to_numpy(dtype=float),
            "spatial_frequency_in_cycles_per_degree": table["spatial_frequency"].to_numpy(dtype=float),
            "phase": table["phase"].to_numpy(dtype=float),
            "is_blank_sweep": is_blank_sweep,
        }
    )


def stim_b_experiment_ids() -> list[int]:
    with open(OPHYS_JSON) as f:
        rows = json.load(f)
    return [int(r["id"]) for r in rows if r.get("stimulus_name") == STIM_B]


def process_one(experiment_id: int, overwrite: bool) -> dict:
    """Cache one experiment's table. Never raises; returns a result dict."""
    out_path = CACHE_DIR / f"{experiment_id}.csv"
    result = {"experiment_id": experiment_id, "status": None, "n_rows": None,
              "n_blank": None, "error": None}
    if out_path.exists() and not overwrite:
        result["status"] = "SKIP"
        return result
    t0 = time.time()
    try:
        df = build_static_gratings_table(experiment_id)
        # Write atomically so a resumed run never sees a half-written file as "done".
        tmp = out_path.with_suffix(".csv.tmp")
        df.to_csv(tmp, index=False)
        tmp.replace(out_path)
        result["status"] = "OK"
        result["n_rows"] = len(df)
        result["n_blank"] = int(df["is_blank_sweep"].sum())
    except Exception as e:
        result["status"] = "FAILED"
        result["error"] = {"type": type(e).__name__, "message": str(e),
                           "traceback": traceback.format_exc()}
    result["elapsed_s"] = round(time.time() - t0, 1)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=4,
                        help="parallel streaming threads (default 4)")
    parser.add_argument("--limit", type=int, default=None,
                        help="only process the first N pending experiments (smoke test)")
    parser.add_argument("--ids", type=int, nargs="*", default=None,
                        help="explicit experiment ids to process (overrides the StimB scan)")
    parser.add_argument("--overwrite", action="store_true",
                        help="re-extract experiments whose cache file already exists")
    args = parser.parse_args()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    ids = args.ids if args.ids is not None else stim_b_experiment_ids()
    if args.limit is not None:
        ids = ids[: args.limit]

    print(f"StimB experiments to consider: {len(ids)} | workers: {args.workers}", flush=True)
    print(f"Cache dir: {CACHE_DIR}", flush=True)

    n_ok = n_skip = n_fail = 0
    failures = []
    t_start = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_one, eid, args.overwrite): eid for eid in ids}
        for n, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            status = res["status"]
            n_ok += status == "OK"
            n_skip += status == "SKIP"
            n_fail += status == "FAILED"
            if status == "FAILED":
                failures.append(res)
            if status != "SKIP":
                detail = (f"{res['n_rows']} rows ({res['n_blank']} blank), {res.get('elapsed_s')}s"
                          if status == "OK"
                          else f"{res['error']['type']}: {res['error']['message']}")
                print(f"[{n}/{len(ids)}] {status:4s} expt {res['experiment_id']}: {detail} "
                      f"| ok={n_ok} skip={n_skip} fail={n_fail}", flush=True)

    print(f"\nFinished {len(ids)} in {(time.time()-t_start)/60:.1f} min. "
          f"ok={n_ok} skip={n_skip} fail={n_fail}", flush=True)
    if failures:
        print("Failures:", flush=True)
        for r in failures:
            print(f"  {r['experiment_id']}: {r['error']['type']}: {r['error']['message']}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
