"""Batch-generate AIND metadata for ALL Visual Behavior Ophys sessions.

Iterates every row of behavior_session_table.csv and writes the full AIND metadata
set (data_description / subject / acquisition / procedures / instrument) into
scratch/vb_ophys_metadata_all/, streaming the NWB file(s) for each session from the
public S3 bucket. A session is one of three kinds, selected per row:

    behavior-only   (ophys_experiment_id is NaN)  -> one behavior NWB
    single-plane 2P (CAM2P.*)                      -> one ophys-experiment NWB
    multi-plane     (MESO.1)                        -> one ophys-experiment NWB per plane

Design (mirrors scripts/run_all_vc_ophys.py)
-------------------------------------------
* Parallelism uses a *process* pool: warnings.catch_warnings is not thread-safe, but
  each worker process has isolated warnings state, so every session's warnings are
  captured and attributed correctly while overlapping the network-bound S3 streaming.
* Every session's Python warnings and any exception (with traceback) are recorded to a
  per-session JSONL report (_report/sessions.jsonl), so "all warnings and errors" are
  documented durably, not just printed. summary.json aggregates them, sliced by
  instrument family so instrument-specific issues are visible.
* Resumable: on restart, behavior_session_ids already recorded OK are skipped;
  previously-failed sessions are retried by default.

Use <= 3 workers: the AIND metadata service returns empty response bodies under higher
concurrency (its procedures endpoint is slow), which surfaces as JSONDecodeError.

With --zip, each session's metadata files are bundled into a single <data asset name>.zip
written to code/metadata_results/visual-behavior-ophys-metadata-only/ (the loose folder
under scratch is removed), so that directory holds only the per-session zips. That is the
deliverable uploaded as the Code Ocean data asset that drives the Zarr conversion (whose
run_conversion.py mounts one such zip per job); mirrors run_all_vb_ephys.py / run_all_vc_*.

Usage
-----
    uv run python scripts/run_all_vb_ophys.py                 # all sessions
    uv run python scripts/run_all_vb_ophys.py --workers 3
    uv run python scripts/run_all_vb_ophys.py --limit 5       # first 5 pending (smoke test)
    uv run python scripts/run_all_vb_ophys.py --indices 0 100 200
    uv run python scripts/run_all_vb_ophys.py --no-retry-failed
    uv run python scripts/run_all_vb_ophys.py --zip           # one zip per session in metadata_results/visual-behavior-ophys-metadata-only
"""
import argparse
import json
import random
import re
import shutil
import sys
import time
import traceback
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from mindscope_to_nwb_zarr.aind_data_schema.utils import (
    DATA_ASSET_NAME_DATETIME_FORMAT,
    get_session_start_time,
    zip_session_metadata,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.metadata_generation import (
    behavior_session_nwb_url,
    ophys_experiment_nwb_url,
    stream_nwb_from_s3,
    generate_behavior_only_session_metadata,
    generate_ophys_session_metadata,
)

HERE = Path(__file__).resolve().parent
CACHE_DIR = HERE.parent.parent / "data" / "visual-behavior-ophys" / "project_metadata"
BEHAVIOR_SESSION_TABLE = CACHE_DIR / "behavior_session_table.csv"
OPHYS_EXPERIMENT_TABLE = CACHE_DIR / "ophys_experiment_table.csv"
# Output goes to the git-ignored code/scratch/ dir (not the tracked scripts/ dir).
OUTPUT_DIR = HERE.parent / "scratch" / "vb_ophys_metadata_all"
REPORT_DIR = OUTPUT_DIR / "_report"
SESSIONS_JSONL = REPORT_DIR / "sessions.jsonl"
SUMMARY_JSON = REPORT_DIR / "summary.json"
# Deliverable directory for the zipped metadata (one zip per session, nothing else). Used
# when --zip is passed: each session's loose folder is written under OUTPUT_DIR and then
# bundled into a single <data asset name>.zip here, so this directory holds only the
# per-session zips (the run report stays in REPORT_DIR, under scratch). The directory name
# matches the conversion's data-asset mount (data/visual-behavior-ophys-metadata-only), so
# the generated zips drive the Zarr conversion directly.
DELIVERABLE_DIR = HERE.parent / "metadata_results" / "visual-behavior-ophys-metadata-only"

# The internal AIND metadata service returns empty/truncated bodies under concurrent
# load, and S3 streaming can drop a connection; both are transient, so retry with
# backoff. Streaming is retried on its own; metadata generation is retried WITHOUT
# re-streaming (the open nwbfile stays valid, and the fetch failure happens before any
# output file is written, so a retry is clean).
MAX_STREAM_ATTEMPTS = 3
MAX_GEN_ATTEMPTS = 5

# A complete session writes all five of these; subject/procedures require the AIND
# metadata service, so their absence signals an unreachable service, not a data gap.
REQUIRED_FILES = ("data_description.json", "subject.json", "acquisition.json",
                  "procedures.json", "instrument.json")


# ---------------------------------------------------------------------------
# Worker (runs in a child process; must be top-level / picklable)
# ---------------------------------------------------------------------------

_BST_CACHE = None
_OET_CACHE = None


def _tables():
    """Load the two session tables once per process."""
    global _BST_CACHE, _OET_CACHE
    if _BST_CACHE is None:
        _BST_CACHE = pd.read_csv(BEHAVIOR_SESSION_TABLE)
        _OET_CACHE = pd.read_csv(OPHYS_EXPERIMENT_TABLE)
    return _BST_CACHE, _OET_CACHE


def _instrument_family(equipment_name) -> str | None:
    """Classify the rig family from equipment_name (BEH / CAM2P / MESO)."""
    if not isinstance(equipment_name, str):
        return None
    for fam in ("BEH", "CAM2P", "MESO"):
        if equipment_name.startswith(fam):
            return fam
    return None


def _missing_required_files(out_dir: Path | None) -> list:
    """Return the required metadata files not present in out_dir (all of them if None)."""
    if out_dir is None or not out_dir.exists():
        return list(REQUIRED_FILES)
    return [f for f in REQUIRED_FILES if not (out_dir / f).exists()]


def _find_output_dir(subject_id: str, session_start_time) -> Path | None:
    """Locate the folder the generator just wrote for this session.

    The folder is named <subject id>_<acq start>_nwb_<packaging date>; subject id plus
    acquisition start is effectively unique per session, so match on that prefix and
    take the most recently written one.
    """
    prefix = f"{subject_id}_{session_start_time.strftime(DATA_ASSET_NAME_DATETIME_FORMAT)}_nwb_"
    matches = list(OUTPUT_DIR.glob(prefix + "*"))
    if not matches:
        return None
    return max(matches, key=lambda p: p.stat().st_mtime)


def _open_session_nwbs(row: pd.Series, oet: pd.DataFrame):
    """Stream the NWB file(s) for one behavior session from S3.

    Returns (is_behavior_only, nwbfiles, session_infos, handles) where handles is a
    list of (io, h5_file, file_handle) tuples the caller must close.
    """
    handles = []
    if pd.isna(row["ophys_experiment_id"]):
        nwbfile, io, h5_file, file_handle = stream_nwb_from_s3(
            behavior_session_nwb_url(int(row["behavior_session_id"]))
        )
        handles.append((io, h5_file, file_handle))
        return True, [nwbfile], [row], handles

    # Behavior + ophys: one NWB per imaging plane. Parse "[id, id, ...]" or a single id.
    ids_str = str(row["ophys_experiment_id"]).strip("[]").strip()
    all_ophys_exp_ids = [int(x.strip()) for x in ids_str.split(",")]

    nwbfiles, session_infos = [], []
    for ophys_experiment_id in all_ophys_exp_ids:
        exp_info = oet.query("ophys_experiment_id == @ophys_experiment_id")
        if len(exp_info) != 1:
            # Fail loud rather than silently dropping a plane: behavior_session_table lists only
            # released (QC-passing) experiments, so every id must resolve to exactly one
            # ophys_experiment_table row (verified across all sessions). A miss means the
            # reference tables are inconsistent, not a QC drop.
            raise RuntimeError(
                f"behavior_session {int(row['behavior_session_id'])}: ophys_experiment_id "
                f"{ophys_experiment_id} has {len(exp_info)} rows in ophys_experiment_table "
                f"(expected exactly 1)"
            )
        nwbfile, io, h5_file, file_handle = stream_nwb_from_s3(
            ophys_experiment_nwb_url(ophys_experiment_id)
        )
        handles.append((io, h5_file, file_handle))
        nwbfiles.append(nwbfile)
        session_infos.append(exp_info.iloc[0])

    if not nwbfiles:
        raise RuntimeError(
            f"No ophys experiment NWB files could be opened for session "
            f"{int(row['behavior_session_id'])}"
        )
    return False, nwbfiles, session_infos, handles


def process_session(index: int, zip_dir: str | None = None) -> dict:
    """Generate metadata for one behavior session (by row index). Never raises.

    Returns a result dict describing status, timing, captured warnings, and (on
    failure) the exception type/message/traceback. When ``zip_dir`` is set, the session's
    metadata files are bundled into a single ``<data asset name>.zip`` in that directory
    (and the loose folder removed), so the deliverable holds only one zip per session.
    """
    bst, oet = _tables()
    row = bst.iloc[index]

    behavior_session_id = int(row["behavior_session_id"])
    equipment_name = row.get("equipment_name")
    subject_id = str(int(row["mouse_id"])) if pd.notna(row.get("mouse_id")) else None

    result = {
        "index": index,
        "behavior_session_id": behavior_session_id,
        "subject_id": subject_id,
        "equipment_name": equipment_name if isinstance(equipment_name, str) else None,
        "instrument_family": _instrument_family(equipment_name),
        "session_type": row.get("session_type") if isinstance(row.get("session_type"), str) else None,
        "is_behavior_only": bool(pd.isna(row["ophys_experiment_id"])),
        "n_planes": None,
        "status": None,
        "output_folder": None,
        "n_files": None,
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
            # Stream the NWB(s) (retry on transient connection failures).
            last_exc = None
            for attempt in range(MAX_STREAM_ATTEMPTS):
                caught[:] = []  # keep only the successful attempt's read-phase warnings
                try:
                    is_behavior_only, nwbfiles, session_infos, handles = _open_session_nwbs(row, oet)
                    break
                except Exception as e:
                    last_exc = e
                    if handles:  # close any partially-opened handles before retrying
                        for h in handles:
                            for x in h:
                                try:
                                    x.close()
                                except Exception:
                                    pass
                        handles = None
                    time.sleep(3 * (attempt + 1))
            else:
                raise last_exc

            result["n_planes"] = len(nwbfiles)
            # Corrected UTC acquisition start (matches the asset-folder name written by
            # data_description; the raw NWB session_start_time is Pacific-local mislabeled
            # UTC on the imaging rigs -- see utils.get_session_start_time).
            start_time = get_session_start_time(nwbfiles[0], session_infos[0])
            # Snapshot warnings emitted while reading the NWB(s) (e.g. hdmf's deprecated
            # schema warnings); the generate loop below clears the buffer per attempt.
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
                    if is_behavior_only:
                        generate_behavior_only_session_metadata(nwbfiles[0], session_infos[0], OUTPUT_DIR)
                    else:
                        generate_ophys_session_metadata(nwbfiles, session_infos, OUTPUT_DIR)
                    out_dir = _find_output_dir(subject_id, start_time)
                    # Completeness gate: subject/procedures fetchers return None (not
                    # raise) when the metadata service is unreachable, leaving a partial
                    # 3-file folder. Treat a missing subject/procedures as a transient
                    # failure -- delete the partial folder and retry -- so incomplete
                    # output is never silently recorded as OK.
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
                result["n_files"] = len(list(out_dir.glob("*.json")))
                if zip_dir is not None:
                    # Bundle the loose folder into a single <data asset name>.zip in the
                    # deliverable dir (and remove the folder), so it holds only the zips.
                    zip_path = zip_session_metadata(out_dir, Path(zip_dir))
                    result["output_folder"] = zip_path.name
                else:
                    result["output_folder"] = out_dir.name
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
                    for x in h:
                        try:
                            x.close()
                        except Exception:
                            pass

        # Record captured warnings: read-phase warnings (snapshotted before the generate
        # loop cleared the buffer) plus the final generate attempt's warnings.
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

def load_done_session_ids(retry_failed: bool) -> set:
    """behavior_session_ids already recorded in sessions.jsonl that should be skipped."""
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
            if rec.get("behavior_session_id") is None:
                continue
            if rec.get("status") == "OK" or (not retry_failed):
                done.add(rec["behavior_session_id"])
    return done


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=3,
                        help="parallel worker processes (default 3)")
    parser.add_argument("--limit", type=int, default=None,
                        help="only process the first N pending sessions (smoke test)")
    parser.add_argument("--indices", type=int, nargs="*", default=None,
                        help="explicit row indices to process (overrides resume/limit)")
    parser.add_argument("--no-retry-failed", dest="retry_failed", action="store_false",
                        help="do not retry sessions previously recorded as FAILED")
    parser.add_argument("--zip", dest="zip", action="store_true",
                        help="bundle each session's files into one zip in "
                             "metadata_results/visual-behavior-ophys-metadata-only (dir holds only the zips)")
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

    bst = pd.read_csv(BEHAVIOR_SESSION_TABLE)
    total = len(bst)

    if args.indices is not None:
        indices = args.indices
    else:
        done = load_done_session_ids(args.retry_failed)
        indices = [i for i in range(total) if int(bst.iloc[i]["behavior_session_id"]) not in done]
        if done:
            print(f"Resuming: {len(done)} sessions already recorded, "
                  f"{len(indices)} pending.", flush=True)
        if args.limit is not None:
            indices = indices[:args.limit]

    print(f"Total sessions: {total} | to process now: {len(indices)} | "
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
        futures = {pool.submit(process_session, i, zip_dir): i for i in indices}
        for n, fut in enumerate(as_completed(futures), 1):
            idx = futures[fut]
            try:
                res = fut.result()
            except Exception as e:  # worker crashed hard (should be rare)
                res = {
                    "index": idx,
                    "behavior_session_id": int(bst.iloc[idx]["behavior_session_id"]),
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
            if status == "OK":
                detail = f"{res.get('n_files')} files, {res.get('n_planes')} plane(s)"
            else:
                err = res.get("error") or {}
                detail = f"{err.get('type')}: {err.get('message')}"
            attempts = res.get("gen_attempts")
            retry_note = f" retry#{attempts}" if attempts and attempts > 1 else ""
            print(f"[{n}/{len(indices)}] {marker} bsid {res.get('behavior_session_id')} "
                  f"subj {res.get('subject_id')} {res.get('equipment_name')} "
                  f"({res.get('elapsed_s')}s, {nw} warn{retry_note}) {detail} "
                  f"| ok={n_ok} fail={n_failed} eta={eta/60:.0f}m", flush=True)

    print(f"\nFinished {len(indices)} in {(time.time()-t_start)/60:.1f} min. "
          f"ok={n_ok} fail={n_failed} warnings_captured={n_warn}", flush=True)
    _write_summary()
    return 0


_DIGITS = re.compile(r"\d+")


def _normalize(msg: str) -> str:
    """Collapse numbers (ids, values) so like warnings/errors group together."""
    return _DIGITS.sub("#", msg or "")


def _write_summary() -> None:
    """Aggregate sessions.jsonl into summary.json (latest record per session)."""
    if not SESSIONS_JSONL.exists():
        return
    latest = {}  # behavior_session_id -> record (keep last occurrence)
    with open(SESSIONS_JSONL) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            latest[rec.get("behavior_session_id")] = rec

    records = list(latest.values())
    ok = [r for r in records if r.get("status") == "OK"]
    failed = [r for r in records if r.get("status") not in ("OK", None)]

    def _by_family(recs) -> dict:
        out = {}
        for r in recs:
            out[r.get("instrument_family")] = out.get(r.get("instrument_family"), 0) + 1
        return out

    # Aggregate warnings by (category, normalized message) -> count + example subjects.
    warn_agg = {}
    for r in records:
        for w in r.get("warnings") or []:
            key = (w.get("category"), _normalize(w.get("message", "")))
            entry = warn_agg.setdefault(key, {"category": w.get("category"),
                                              "message_pattern": key[1],
                                              "count": 0, "example_subjects": [],
                                              "instrument_families": []})
            entry["count"] += 1
            if len(entry["example_subjects"]) < 10 and r.get("subject_id") not in entry["example_subjects"]:
                entry["example_subjects"].append(r.get("subject_id"))
            fam = r.get("instrument_family")
            if fam not in entry["instrument_families"]:
                entry["instrument_families"].append(fam)

    # Aggregate errors by (type, normalized message).
    err_agg = {}
    for r in failed:
        e = r.get("error") or {}
        key = (e.get("type"), _normalize(e.get("message", "")))
        entry = err_agg.setdefault(key, {"type": e.get("type"), "message_pattern": key[1],
                                         "count": 0, "example_sessions": []})
        entry["count"] += 1
        if len(entry["example_sessions"]) < 20:
            entry["example_sessions"].append(r.get("behavior_session_id"))

    summary = {
        "total_recorded": len(records),
        "ok": len(ok),
        "failed": len(failed),
        "ok_by_instrument_family": _by_family(ok),
        "failed_by_instrument_family": _by_family(failed),
        "sessions_with_warnings": sum(1 for r in records if r.get("warnings")),
        "total_warnings": sum(len(r.get("warnings") or []) for r in records),
        "warning_types": sorted(warn_agg.values(), key=lambda d: -d["count"]),
        "error_types": sorted(err_agg.values(), key=lambda d: -d["count"]),
        "failed_sessions": [
            {"behavior_session_id": r.get("behavior_session_id"), "subject_id": r.get("subject_id"),
             "equipment_name": r.get("equipment_name"),
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
    print(f"  OK by family:     {summary['ok_by_instrument_family']}", flush=True)
    print(f"  FAILED by family: {summary['failed_by_instrument_family']}", flush=True)
    if summary["error_types"]:
        print("  Error types:", flush=True)
        for e in summary["error_types"]:
            print(f"    x{e['count']:4d}  {e['type']}: {e['message_pattern'][:100]}", flush=True)
    if summary["warning_types"]:
        print("  Warning types:", flush=True)
        for w in summary["warning_types"]:
            print(f"    x{w['count']:4d}  {w['category']} {w.get('instrument_families')}: "
                  f"{w['message_pattern'][:90]}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
