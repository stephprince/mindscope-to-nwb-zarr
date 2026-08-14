"""Batch-generate AIND metadata for ALL Visual Coding ophys sessions.

Streams every processed NWB in ophys_experiments.json from DANDI and writes the
full AIND metadata set (data_description / subject / acquisition / procedures /
instrument) into scratch/vc_ophys_metadata_test/, exactly as the single-session
scripts do -- just over all ~1518 experiments.

Design
------
* Parallelism uses a *process* pool (not threads): warnings.catch_warnings is not
  thread-safe, but each worker process has isolated warnings state, so every
  session's warnings are captured and attributed correctly while still overlapping
  the network-bound DANDI streaming across workers.
* Every session's Python warnings and any exception (with full traceback) are
  recorded to a per-session JSONL report (_report/sessions.jsonl), so "all warnings
  and errors" are documented durably, not just printed.
* Resumable: on restart, experiments already recorded OK in sessions.jsonl are
  skipped. Non-OK (failed) experiments are retried by default.

Use <= 3 workers: the AIND metadata service returns empty response bodies under higher
concurrency (its procedures endpoint is slow), which surfaces as JSONDecodeError.

Usage
-----
    uv run python scripts/run_all_vc_ophys.py                 # all sessions
    uv run python scripts/run_all_vc_ophys.py --zip           # one zip per session in metadata_results/visual-coding-ophys-metadata-only
    uv run python scripts/run_all_vc_ophys.py --workers 3
    uv run python scripts/run_all_vc_ophys.py --limit 5       # first 5 pending (smoke test)
    uv run python scripts/run_all_vc_ophys.py --indices 50 200 350
    uv run python scripts/run_all_vc_ophys.py --no-retry-failed

With --zip, each session's five metadata files are bundled into a single
<data asset name>.zip written to code/metadata_results/visual-coding-ophys-metadata-only/ (the loose
folder is written under scratch and the run report stays in _report), so that directory
ends up holding only the per-session zips.
"""
import argparse
import json
import os
import random
import shutil
import sys
import time
import traceback
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from mindscope_to_nwb_zarr.aind_data_schema.utils import DATA_ASSET_NAME_DATETIME_FORMAT
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ophys.metadata_generation import (
    get_dandi_asset_path,
    stream_nwb_from_dandi,
    generate_session_metadata,
    zip_session_metadata,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ophys.instrument import (
    rig_for_experiment,
)

HERE = Path(__file__).resolve().parent
OPHYS_JSON = HERE.parent.parent / "data" / "allen-brain-observatory" / "visual-coding-2p" / "ophys_experiments.json"
# Output goes to the git-ignored code/scratch/ dir (not the tracked scripts/ dir).
OUTPUT_DIR = HERE.parent / "scratch" / "vc_ophys_metadata_test"
REPORT_DIR = OUTPUT_DIR / "_report"
SESSIONS_JSONL = REPORT_DIR / "sessions.jsonl"
SUMMARY_JSON = REPORT_DIR / "summary.json"

# Deliverable directory for the zipped metadata (one zip per session, nothing else).
# Used when --zip is passed: each session's loose folder is written under OUTPUT_DIR and
# then bundled into a single zip here, so this directory holds only the per-session zips.
# The run report stays in REPORT_DIR (under scratch), so the deliverable stays only-zips.
DELIVERABLE_DIR = HERE.parent / "metadata_results" / "visual-coding-ophys-metadata-only"

# The internal AIND metadata service returns empty/truncated bodies under concurrent
# load (subject + procedures fetches, each with a raw-parse fallback = up to 4 calls
# per session). Streaming the NWB from DANDI can also drop a connection. Both are
# transient, so retry with backoff. Streaming is retried on its own; metadata
# generation is retried WITHOUT re-streaming (the open nwbfile stays valid, and the
# fetch failure happens before any output file is written, so a retry is clean).
MAX_STREAM_ATTEMPTS = 3
MAX_GEN_ATTEMPTS = 5


# ---------------------------------------------------------------------------
# Worker (runs in a child process; must be top-level / picklable)
# ---------------------------------------------------------------------------

_ROWS_CACHE = None

# A complete session writes all five of these; subject/procedures require the AIND
# metadata service, so their absence signals an unreachable service, not a data gap.
REQUIRED_FILES = ("data_description.json", "subject.json", "acquisition.json",
                  "procedures.json", "instrument.json")


def _missing_required_files(out_dir: Path | None) -> list:
    """Return the required metadata files not present in out_dir (all of them if None)."""
    if out_dir is None or not out_dir.exists():
        return list(REQUIRED_FILES)
    return [f for f in REQUIRED_FILES if not (out_dir / f).exists()]


def _rows():
    """Load ophys_experiments.json once per process."""
    global _ROWS_CACHE
    if _ROWS_CACHE is None:
        with open(OPHYS_JSON) as f:
            _ROWS_CACHE = json.load(f)
    return _ROWS_CACHE


def _find_output_dir(subject_id: str, session_start_time) -> Path | None:
    """Locate the folder generate_session_metadata just wrote for this session.

    The folder is named <subject id>_<acq start>_nwb_<packaging date>; subject id
    plus acquisition start is effectively unique per session, so match on that prefix
    and take the most recently written one.
    """
    prefix = f"{subject_id}_{session_start_time.strftime(DATA_ASSET_NAME_DATETIME_FORMAT)}_nwb_"
    matches = list(OUTPUT_DIR.glob(prefix + "*"))
    if not matches:
        return None
    return max(matches, key=lambda p: p.stat().st_mtime)


def process_experiment(index: int, zip_dir: str | None = None) -> dict:
    """Generate metadata for one experiment (by row index). Never raises.

    Returns a result dict describing status, timing, captured warnings, and (on
    failure) the exception type/message/traceback. When ``zip_dir`` is set, the session's
    five metadata files are bundled into a single ``<data asset name>.zip`` in that
    directory (and the loose folder removed), so the deliverable holds only one zip/session.
    """
    row = _rows()[index]
    session_info = pd.Series(row)

    experiment_id = row.get("id")
    specimen_id = row.get("specimen_id")
    stimulus_name = row.get("stimulus_name")
    try:
        subject_id = row["specimen"]["donor"]["external_donor_name"]
    except Exception:
        subject_id = None
    try:
        rig = rig_for_experiment(session_info)
    except Exception:
        rig = None

    result = {
        "index": index,
        "experiment_id": experiment_id,
        "specimen_id": specimen_id,
        "subject_id": subject_id,
        "rig": rig,
        "stimulus_name": stimulus_name,
        "asset_path": None,
        "status": None,
        "output_folder": None,
        "n_files": None,
        "n_stimulus_epochs": None,
        "elapsed_s": None,
        "gen_attempts": None,
        "warnings": [],
        "error": None,
    }

    # Small stagger so N workers don't hit the metadata service in lockstep bursts.
    time.sleep(random.uniform(0, 3))

    t0 = time.time()
    handles = None
    stream_warnings = []  # read-phase warnings, snapshotted before the generate loop clears the buffer
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")  # capture every warning, incl. duplicates
        try:
            asset_path = get_dandi_asset_path(session_info)
            result["asset_path"] = asset_path

            # Stream the NWB (retry on transient connection failures).
            last_exc = None
            for attempt in range(MAX_STREAM_ATTEMPTS):
                caught[:] = []  # keep only the successful attempt's read-phase warnings
                try:
                    nwbfile, io, h5_file, file_handle = stream_nwb_from_dandi(asset_path)
                    handles = (io, h5_file, file_handle)
                    break
                except Exception as e:
                    last_exc = e
                    time.sleep(3 * (attempt + 1))
            else:
                raise last_exc
            start_time = nwbfile.session_start_time
            # Snapshot warnings emitted while reading the NWB (e.g. hdmf's deprecated
            # Device.manufacturer schema warning). The generate loop below clears the
            # buffer per attempt, so without this snapshot these would be lost.
            stream_warnings = list(caught)

            # Generate metadata (retry on transient empty API responses). Clear the
            # captured-warnings buffer before each attempt so only the attempt that
            # ultimately runs contributes warnings (no inflation from retried attempts).
            last_exc = None
            out_dir = None
            for attempt in range(MAX_GEN_ATTEMPTS):
                caught[:] = []
                result["gen_attempts"] = attempt + 1
                try:
                    generate_session_metadata(nwbfile, session_info, OUTPUT_DIR)
                    out_dir = _find_output_dir(subject_id, start_time)
                    # Completeness gate: the subject/procedures fetchers return None
                    # (not raise) when the metadata service is unreachable, which would
                    # leave a partial 3-file folder. Treat a missing subject.json or
                    # procedures.json as a transient failure -- delete the partial folder
                    # and retry -- so we never silently record incomplete output as OK.
                    missing = _missing_required_files(out_dir)
                    if missing:
                        if out_dir is not None and out_dir.exists():
                            shutil.rmtree(out_dir, ignore_errors=True)
                        out_dir = None
                        raise RuntimeError(
                            "incomplete metadata (metadata service unreachable?), "
                            f"missing: {', '.join(missing)}"
                        )
                    last_exc = None
                    break
                except Exception as e:
                    last_exc = e
                    time.sleep(4 * (attempt + 1))
            if last_exc is not None:
                raise last_exc

            if out_dir is not None:
                result["output_folder"] = out_dir.name
                json_files = sorted(out_dir.glob("*.json"))
                result["n_files"] = len(json_files)
                acq_path = out_dir / "acquisition.json"
                if acq_path.exists():
                    with open(acq_path) as f:
                        acq = json.load(f)
                    result["n_stimulus_epochs"] = len(acq.get("stimulus_epochs", []))
                # In zip mode, bundle the folder into a single zip in the deliverable dir
                # (done after the reads above, since it removes the loose folder).
                if zip_dir is not None:
                    zip_path = zip_session_metadata(out_dir, Path(zip_dir))
                    result["output_folder"] = zip_path.name
            result["status"] = "OK"
        except Exception as e:
            result["status"] = "FAILED"
            result["error"] = {
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc(),
            }
        finally:
            if handles is not None:
                for h in handles:
                    try:
                        h.close()
                    except Exception:
                        pass

        # Record captured warnings: read-phase warnings (snapshotted before the
        # generate loop cleared the buffer) plus the final generate attempt's warnings.
        for w in stream_warnings + list(caught):
            result["warnings"].append({
                "category": w.category.__name__,
                "message": str(w.message),
                "filename": w.filename,
                "lineno": w.lineno,
            })

    result["elapsed_s"] = round(time.time() - t0, 1)
    return result


# ---------------------------------------------------------------------------
# Driver (parent process)
# ---------------------------------------------------------------------------

def load_done_experiment_ids(retry_failed: bool) -> set:
    """Experiment ids already recorded in sessions.jsonl that should be skipped."""
    done = set()
    if not SESSIONS_JSONL.exists():
        return done
    with open(SESSIONS_JSONL) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("experiment_id") is None:
                continue
            if rec.get("status") == "OK" or (not retry_failed):
                done.add(rec["experiment_id"])
    return done


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=6,
                        help="parallel worker processes (default 6)")
    parser.add_argument("--limit", type=int, default=None,
                        help="only process the first N pending experiments (smoke test)")
    parser.add_argument("--indices", type=int, nargs="*", default=None,
                        help="explicit row indices to process (overrides resume/limit)")
    parser.add_argument("--no-retry-failed", dest="retry_failed", action="store_false",
                        help="do not retry experiments previously recorded as FAILED")
    parser.add_argument("--zip", dest="zip", action="store_true",
                        help="bundle each session's 5 files into one zip in "
                             "metadata_results/visual-coding-ophys-metadata-only (dir holds only the zips)")
    parser.set_defaults(retry_failed=True)
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    # In zip mode the deliverable dir gets only the per-session zips; loose folders are
    # written under OUTPUT_DIR (scratch) then zipped away, and the report stays in REPORT_DIR.
    zip_dir = None
    if args.zip:
        DELIVERABLE_DIR.mkdir(parents=True, exist_ok=True)
        zip_dir = str(DELIVERABLE_DIR)

    rows = json.load(open(OPHYS_JSON))
    total = len(rows)

    if args.indices is not None:
        indices = args.indices
    else:
        done = load_done_experiment_ids(args.retry_failed)
        indices = [i for i in range(total) if rows[i].get("id") not in done]
        if done:
            print(f"Resuming: {len(done)} experiments already recorded, "
                  f"{len(indices)} pending.", flush=True)
        if args.limit is not None:
            indices = indices[:args.limit]

    print(f"Total experiments: {total} | to process now: {len(indices)} | "
          f"workers: {args.workers}", flush=True)
    print(f"Output:  {DELIVERABLE_DIR if zip_dir else OUTPUT_DIR}{' (zips)' if zip_dir else ''}", flush=True)
    print(f"Report:  {SESSIONS_JSONL}", flush=True)
    if not indices:
        print("Nothing to do.", flush=True)
        _write_summary()
        return 0

    n_ok = n_failed = n_warn = 0
    t_start = time.time()
    # Append per-session records as they complete so progress is durable/resumable.
    with open(SESSIONS_JSONL, "a", encoding="utf-8") as jsonl, \
            ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_experiment, i, zip_dir): i for i in indices}
        for n, fut in enumerate(as_completed(futures), 1):
            idx = futures[fut]
            try:
                res = fut.result()
            except Exception as e:  # worker crashed hard (should be rare)
                res = {
                    "index": idx,
                    "experiment_id": rows[idx].get("id"),
                    "status": "WORKER_CRASH",
                    "error": {"type": type(e).__name__, "message": str(e),
                              "traceback": traceback.format_exc()},
                    "warnings": [],
                }
            jsonl.write(json.dumps(res) + "\n")
            jsonl.flush()

            status = res.get("status")
            n_ok += status == "OK"
            n_failed += status != "OK"
            nw = len(res.get("warnings") or [])
            n_warn += nw

            elapsed = time.time() - t_start
            rate = n / elapsed if elapsed else 0
            eta = (len(indices) - n) / rate if rate else 0
            marker = "OK  " if status == "OK" else "FAIL"
            detail = ""
            if status == "OK":
                detail = f"{res.get('n_files')} files, {res.get('n_stimulus_epochs')} epochs"
            else:
                err = res.get("error") or {}
                detail = f"{err.get('type')}: {err.get('message')}"
            attempts = res.get("gen_attempts")
            retry_note = f" retry#{attempts}" if attempts and attempts > 1 else ""
            print(f"[{n}/{len(indices)}] {marker} expt {res.get('experiment_id')} "
                  f"subj {res.get('subject_id')} {res.get('rig')} "
                  f"({res.get('elapsed_s')}s, {nw} warn{retry_note}) {detail} "
                  f"| ok={n_ok} fail={n_failed} eta={eta/60:.0f}m", flush=True)

    print(f"\nFinished {len(indices)} in {(time.time()-t_start)/60:.1f} min. "
          f"ok={n_ok} fail={n_failed} warnings_captured={n_warn}", flush=True)
    _write_summary()
    return 0


def _write_summary() -> None:
    """Aggregate sessions.jsonl into summary.json (latest record per experiment)."""
    if not SESSIONS_JSONL.exists():
        return
    latest = {}  # experiment_id -> record (keep last occurrence)
    with open(SESSIONS_JSONL) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            latest[rec.get("experiment_id")] = rec

    records = list(latest.values())
    ok = [r for r in records if r.get("status") == "OK"]
    failed = [r for r in records if r.get("status") not in ("OK", None)]

    # Aggregate warnings by (category, normalized message) -> count + example subjects.
    warn_agg = {}
    for r in records:
        for w in r.get("warnings") or []:
            key = (w.get("category"), _normalize(w.get("message", "")))
            entry = warn_agg.setdefault(key, {"category": w.get("category"),
                                              "message_pattern": key[1],
                                              "count": 0, "example_subjects": []})
            entry["count"] += 1
            if len(entry["example_subjects"]) < 10 and r.get("subject_id") not in entry["example_subjects"]:
                entry["example_subjects"].append(r.get("subject_id"))

    # Aggregate errors by (type, normalized message).
    err_agg = {}
    for r in failed:
        e = r.get("error") or {}
        key = (e.get("type"), _normalize(e.get("message", "")))
        entry = err_agg.setdefault(key, {"type": e.get("type"), "message_pattern": key[1],
                                         "count": 0, "example_experiments": []})
        entry["count"] += 1
        if len(entry["example_experiments"]) < 20:
            entry["example_experiments"].append(r.get("experiment_id"))

    summary = {
        "total_recorded": len(records),
        "ok": len(ok),
        "failed": len(failed),
        "sessions_with_warnings": sum(1 for r in records if r.get("warnings")),
        "total_warnings": sum(len(r.get("warnings") or []) for r in records),
        "warning_types": sorted(warn_agg.values(), key=lambda d: -d["count"]),
        "error_types": sorted(err_agg.values(), key=lambda d: -d["count"]),
        "failed_experiments": [
            {"experiment_id": r.get("experiment_id"), "subject_id": r.get("subject_id"),
             "error": (r.get("error") or {}).get("type"),
             "message": (r.get("error") or {}).get("message")}
            for r in failed
        ],
    }
    with open(SUMMARY_JSON, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSummary written to {SUMMARY_JSON}", flush=True)
    print(f"  OK={summary['ok']} FAILED={summary['failed']} "
          f"sessions_with_warnings={summary['sessions_with_warnings']} "
          f"total_warnings={summary['total_warnings']}", flush=True)
    if summary["error_types"]:
        print("  Error types:", flush=True)
        for e in summary["error_types"]:
            print(f"    x{e['count']:4d}  {e['type']}: {e['message_pattern'][:100]}", flush=True)
    if summary["warning_types"]:
        print("  Warning types:", flush=True)
        for w in summary["warning_types"]:
            print(f"    x{w['count']:4d}  {w['category']}: {w['message_pattern'][:100]}", flush=True)


import re

_DIGITS = re.compile(r"\d+")


def _normalize(msg: str) -> str:
    """Collapse numbers (ids, values) so like warnings/errors group together."""
    return _DIGITS.sub("#", msg or "")


if __name__ == "__main__":
    sys.exit(main())
