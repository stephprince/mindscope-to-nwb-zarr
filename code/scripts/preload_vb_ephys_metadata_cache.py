"""Preload the AIND metadata-service cache for all Visual Behavior Neuropixels subjects.

Like scripts/preload_aind_metadata_cache.py (Visual Behavior Ophys), but for the Visual
Behavior *Ephys* (neuropixels) dataset. The dataset has thousands of sessions but only a
few hundred subjects, and a subject's metadata-service response does not change between
runs, so this fetches each unique subject's subject + procedures records once and writes
them to the shared on-disk cache (see
mindscope_to_nwb_zarr.aind_data_schema.metadata_service_cache, keyed by the 6-digit mouse
id). After preloading, run_all_vb_ephys.py hits the cache for subject/procedures instead
of calling the slow metadata service per session, and can run with more workers.

Subject id source: the ``mouse_id`` column of both VBN session tables
(behavior_sessions.csv unioned with ecephys_sessions.csv). These 6-digit ids are the key
the metadata service expects, and match nwbfile.subject.subject_id used at fetch time.

The raw fetchers are pipeline-independent (the service is keyed by the 6-digit id and
returns the same record), so the Visual Behavior Ephys fetcher pair is used here.

Usage
-----
    uv run python scripts/preload_vb_ephys_metadata_cache.py
    uv run python scripts/preload_vb_ephys_metadata_cache.py --workers 3
    uv run python scripts/preload_vb_ephys_metadata_cache.py --force   # refetch even if cached
"""
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from mindscope_to_nwb_zarr.aind_data_schema.metadata_service_cache import (
    cache_dir, load_cached, store_cached, SUBJECT, PROCEDURES,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.subject import _fetch_subject_raw
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.procedures import _fetch_procedures_raw

HERE = Path(__file__).resolve().parent
PROJECT_METADATA = HERE.parent.parent / "data" / "visual-behavior-neuropixels" / "project_metadata"
BEHAVIOR_SESSIONS = PROJECT_METADATA / "behavior_sessions.csv"
ECEPHYS_SESSIONS = PROJECT_METADATA / "ecephys_sessions.csv"
API_HOST = "http://aind-metadata-service"

_FETCHERS = {SUBJECT: _fetch_subject_raw, PROCEDURES: _fetch_procedures_raw}


def unique_subject_ids() -> list[str]:
    """All unique subject_ids (6-digit mouse ids) across the two VBN session tables.

    Every subject appears in behavior_sessions; the ecephys table is unioned in for
    safety. Ids are stringified to match nwbfile.subject.subject_id used at fetch time.
    """
    behavior = pd.read_csv(BEHAVIOR_SESSIONS)
    ecephys = pd.read_csv(ECEPHYS_SESSIONS)
    ids = set(behavior["mouse_id"].dropna().astype(int)) | set(ecephys["mouse_id"].dropna().astype(int))
    return sorted(str(i) for i in ids)


def preload_one(kind: str, subject_id: str, force: bool) -> tuple[str, str, str, str | None]:
    """Fetch+cache one (kind, subject_id). Returns (kind, subject_id, status, detail)."""
    if not force and load_cached(kind, subject_id) is not None:
        return kind, subject_id, "cached", None
    try:
        raw = _FETCHERS[kind](subject_id, API_HOST)
    except RuntimeError as e:
        # The raw fetchers raise RuntimeError for a genuine 404, a 5xx service outage, and
        # a non-JSON body. Classify by message so a service outage is not mislabeled as a
        # missing record.
        msg = str(e)
        if "404" in msg:
            return kind, subject_id, "missing_404", msg
        return kind, subject_id, "service_error", msg
    except Exception as e:  # noqa: BLE001 -- record any other fetch failure
        return kind, subject_id, f"error:{type(e).__name__}", str(e)
    if raw is None:
        return kind, subject_id, "unreachable", None
    store_cached(kind, subject_id, raw)
    return kind, subject_id, "fetched", None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=3,
                        help="parallel fetch threads (default 3; the metadata service empties "
                             "responses above ~3 workers, so keep it low)")
    parser.add_argument("--force", action="store_true",
                        help="refetch and overwrite even if already cached")
    args = parser.parse_args()

    subject_ids = unique_subject_ids()
    tasks = [(kind, sid) for sid in subject_ids for kind in (SUBJECT, PROCEDURES)]
    print(f"Subjects: {len(subject_ids)} | fetches: {len(tasks)} "
          f"(subject + procedures) | workers: {args.workers}", flush=True)
    print(f"Cache dir: {cache_dir()}", flush=True)

    counts: dict[str, int] = {}
    problems: list[tuple[str, str, str, str | None]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(preload_one, kind, sid, args.force): (kind, sid)
                   for kind, sid in tasks}
        for n, fut in enumerate(as_completed(futures), 1):
            kind, sid, status, detail = fut.result()
            counts[status] = counts.get(status, 0) + 1
            if status not in ("fetched", "cached"):
                problems.append((kind, sid, status, detail))
            if n % 25 == 0 or n == len(tasks):
                print(f"  [{n}/{len(tasks)}] {dict(sorted(counts.items()))}", flush=True)

    print(f"\nDone. {dict(sorted(counts.items()))}", flush=True)
    if problems:
        # One example detail per (kind, status) keeps the outage obvious without 81 lines.
        print(f"{len(problems)} problem fetch(es). Examples by kind/status:", flush=True)
        seen = set()
        for kind, sid, status, detail in problems:
            key = (kind, status)
            if key in seen:
                continue
            seen.add(key)
            print(f"  {status:16} {kind:10} e.g. subject {sid}: {detail}", flush=True)
    # Non-zero exit if the service was unreachable or erroring, so a wrapper can retry.
    return 1 if any(p[2] in ("unreachable", "service_error") for p in problems) else 0


if __name__ == "__main__":
    sys.exit(main())
