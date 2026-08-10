"""Preload the AIND metadata-service cache for all Visual Coding subjects (ophys + ephys).

Like scripts/preload_aind_metadata_cache.py (Visual Behavior), but for the two Visual
Coding pipelines. The metadata-service response for a subject does not change between runs
and the same subject recurs across many sessions, so this fetches each unique subject's
subject + procedures records once and writes them to the shared on-disk cache
(see mindscope_to_nwb_zarr.aind_data_schema.metadata_service_cache, keyed by the 6-digit
mouse id). After preloading, the Visual Coding ophys/ephys runs hit the cache for
subject/procedures instead of calling the slow metadata service per session.

Subject id sources (all 6-digit mouse ids, the key the metadata service expects):
  * Visual Coding ophys:  external_donor_name of every experiment in ophys_experiments.json
  * Visual Coding ephys:  the values of the NWB-subject-id -> mouse-id mapping JSON,
                          unioned with the mouse_id column of the experiment metadata CSV

The raw fetchers are pipeline-independent (the service is keyed by the 6-digit id and
returns the same record), so a single fetcher pair is used for every subject.

Usage
-----
    uv run python scripts/preload_vc_metadata_cache.py
    uv run python scripts/preload_vc_metadata_cache.py --workers 4
    uv run python scripts/preload_vc_metadata_cache.py --force   # refetch even if cached
"""
import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from mindscope_to_nwb_zarr.aind_data_schema.metadata_service_cache import (
    cache_dir, load_cached, store_cached, SUBJECT, PROCEDURES,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.subject import _fetch_subject_raw
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.procedures import _fetch_procedures_raw

HERE = Path(__file__).resolve().parent
CODE_DIR = HERE.parent
DATA_DIR = CODE_DIR.parent / "data"
OPHYS_JSON = DATA_DIR / "allen-brain-observatory" / "visual-coding-2p" / "ophys_experiments.json"
EPHYS_SUBJECT_MAPPING = CODE_DIR / "reference" / "visual_coding_ephys_subject_mapping.json"
EPHYS_EXPERIMENT_CSV = CODE_DIR / "reference" / "neuropixels_vc_experiment_metadata.csv"
API_HOST = "http://aind-metadata-service"

_FETCHERS = {SUBJECT: _fetch_subject_raw, PROCEDURES: _fetch_procedures_raw}


def ophys_subject_ids() -> set[str]:
    """6-digit donor ids of every Visual Coding ophys experiment."""
    with open(OPHYS_JSON) as f:
        rows = json.load(f)
    return {str(r["specimen"]["donor"]["external_donor_name"]) for r in rows}


def ephys_subject_ids() -> set[str]:
    """6-digit mouse ids for Visual Coding ephys (mapping values, unioned with the CSV)."""
    ids: set[str] = set()
    with open(EPHYS_SUBJECT_MAPPING) as f:
        ids |= {str(v) for v in json.load(f).values()}
    csv = pd.read_csv(EPHYS_EXPERIMENT_CSV)
    ids |= {str(int(m)) for m in csv["mouse_id"].dropna()}
    return ids


def unique_subject_ids() -> list[str]:
    """All unique 6-digit subject ids across the two Visual Coding pipelines."""
    return sorted(ophys_subject_ids() | ephys_subject_ids())


def preload_one(kind: str, subject_id: str, force: bool) -> tuple[str, str, str]:
    """Fetch+cache one (kind, subject_id). Returns (kind, subject_id, status)."""
    if not force and load_cached(kind, subject_id) is not None:
        return kind, subject_id, "cached"
    try:
        raw = _FETCHERS[kind](subject_id, API_HOST)
    except RuntimeError:  # HTTP 404 -- no record for this subject
        return kind, subject_id, "missing_404"
    except Exception as e:  # noqa: BLE001 -- record any other fetch failure
        return kind, subject_id, f"error:{type(e).__name__}"
    if raw is None:
        return kind, subject_id, "unreachable"
    store_cached(kind, subject_id, raw)
    return kind, subject_id, "fetched"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=["ophys", "ephys", "both"], default="both",
                        help="which Visual Coding pipeline's subjects to preload (default both)")
    parser.add_argument("--workers", type=int, default=4,
                        help="parallel fetch threads (default 4; keep low for the metadata service)")
    parser.add_argument("--force", action="store_true",
                        help="refetch and overwrite even if already cached")
    args = parser.parse_args()

    if args.dataset == "ophys":
        subject_ids = sorted(ophys_subject_ids())
    elif args.dataset == "ephys":
        subject_ids = sorted(ephys_subject_ids())
    else:
        subject_ids = unique_subject_ids()
    tasks = [(kind, sid) for sid in subject_ids for kind in (SUBJECT, PROCEDURES)]
    print(f"Dataset: {args.dataset} | subjects: {len(subject_ids)} | "
          f"fetches: {len(tasks)} (subject + procedures) | workers: {args.workers}", flush=True)
    print(f"Cache dir: {cache_dir()}", flush=True)

    counts: dict[str, int] = {}
    problems: list[tuple[str, str, str]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(preload_one, kind, sid, args.force): (kind, sid)
                   for kind, sid in tasks}
        for n, fut in enumerate(as_completed(futures), 1):
            kind, sid, status = fut.result()
            counts[status] = counts.get(status, 0) + 1
            if status not in ("fetched", "cached"):
                problems.append((kind, sid, status))
            if n % 25 == 0 or n == len(tasks):
                print(f"  [{n}/{len(tasks)}] {dict(sorted(counts.items()))}", flush=True)

    print(f"\nDone. {dict(sorted(counts.items()))}", flush=True)
    if problems:
        print(f"{len(problems)} problem fetch(es):", flush=True)
        for kind, sid, status in problems:
            print(f"  {status:16} {kind:10} subject {sid}", flush=True)
    # Non-zero exit if the service was unreachable, so a wrapper can retry.
    return 1 if any(p[2] == "unreachable" for p in problems) else 0


if __name__ == "__main__":
    sys.exit(main())
