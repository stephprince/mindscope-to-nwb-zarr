"""Preload the AIND metadata-service cache for all Visual Behavior Ophys subjects.

The Visual Behavior Ophys dataset has ~4800 sessions but only ~107 subjects, and a
subject's metadata-service response does not change between runs. This fetches each
unique subject's subject + procedures records once and writes them to the on-disk cache
(see mindscope_to_nwb_zarr.aind_data_schema.metadata_service_cache). After preloading,
run_all_vb_ophys.py hits the cache for subject/procedures (no per-session network calls
to the slow metadata service) and can safely run with many workers.

Usage
-----
    uv run python scripts/preload_vb_ophys_metadata_cache.py
    uv run python scripts/preload_vb_ophys_metadata_cache.py --workers 4
    uv run python scripts/preload_vb_ophys_metadata_cache.py --force   # refetch even if cached
"""
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from mindscope_to_nwb_zarr.aind_data_schema.metadata_service_cache import (
    cache_dir, load_cached, store_cached, SUBJECT, PROCEDURES,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.subject import _fetch_subject_raw
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.procedures import _fetch_procedures_raw

HERE = Path(__file__).resolve().parent
CACHE_DIR = HERE.parent.parent / "data" / "visual-behavior-ophys" / "project_metadata"
API_HOST = "http://aind-metadata-service"

_FETCHERS = {SUBJECT: _fetch_subject_raw, PROCEDURES: _fetch_procedures_raw}


def unique_subject_ids() -> list[str]:
    """All unique subject_ids (6-digit mouse ids) across the two session tables.

    Every subject appears in behavior_session_table; the ophys table is unioned in for
    safety. Ids are stringified to match nwbfile.subject.subject_id used at fetch time.
    """
    bst = pd.read_csv(CACHE_DIR / "behavior_session_table.csv")
    oet = pd.read_csv(CACHE_DIR / "ophys_experiment_table.csv")
    ids = set(bst["mouse_id"].dropna().astype(int)) | set(oet["mouse_id"].dropna().astype(int))
    return sorted(str(i) for i in ids)


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
    parser.add_argument("--workers", type=int, default=4,
                        help="parallel fetch threads (default 4; keep low for the metadata service)")
    parser.add_argument("--force", action="store_true",
                        help="refetch and overwrite even if already cached")
    args = parser.parse_args()

    subject_ids = unique_subject_ids()
    tasks = [(kind, sid) for sid in subject_ids for kind in (SUBJECT, PROCEDURES)]
    print(f"Subjects: {len(subject_ids)} | fetches: {len(tasks)} "
          f"(subject + procedures) | workers: {args.workers}", flush=True)
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
