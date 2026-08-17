"""Batch-generate AIND metadata for ALL Visual Behavior Neuropixels (ephys) sessions.

Streams every session NWB from the public visual-behavior-neuropixels-data S3 bucket over
HTTPS (no download / no mount) and writes the AIND metadata set into
scratch/vb_ephys_metadata_all/, then records every session's warnings and any error to a
durable JSONL report (_report/sessions.jsonl) plus an aggregated summary.json. This is the
Visual Behavior counterpart of scripts/run_all_vc_ephys.py, and mirrors run_all_vb_ophys.py.

Sessions come from behavior_sessions.csv. A behavior session that has an associated ecephys
session (its behavior_session_id appears in ecephys_sessions.csv) is processed as an
*ecephys* session (the larger ecephys NWB + the ecephys table row); otherwise it is a
*behavior-only* session. The production pipeline
(visual_behavior_ephys/metadata_generation.py) makes exactly this choice from a mounted S3
bucket; locally we stream instead and drive the same production generator + fetch functions.

Because visual_behavior_ephys has no instrument module yet, the metadata set is four files
(data_description / subject / acquisition / procedures); instrument.json is intentionally
not expected. subject/procedures are fail-loud: an unreachable metadata service or an
NWB/LIMS disagreement raises, which the retry loop / completeness gate surface as a FAILED
session rather than silently writing partial output.

Design mirrors run_all_vb_ophys.py: a process pool (warnings.catch_warnings is not
thread-safe, but each worker process has isolated warnings state), per-session JSONL
records, resumable on restart, retry-with-backoff on transient service/stream failures, and
a completeness gate that never records partial output as OK.

Preload the metadata-service cache first (scripts/preload_vb_ephys_metadata_cache.py) so
subject/procedures come from the on-disk cache and the run can use more workers; otherwise
keep --workers low, as the AIND metadata service returns empty bodies under concurrency.

Usage
-----
    uv run python scripts/run_all_vb_ephys.py                 # all sessions
    uv run python scripts/run_all_vb_ephys.py --workers 3
    uv run python scripts/run_all_vb_ephys.py --limit 5       # first 5 pending (smoke test)
    uv run python scripts/run_all_vb_ephys.py --indices 0 100 200
    uv run python scripts/run_all_vb_ephys.py --no-retry-failed
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

import h5py
import pandas as pd
import remfile
from pynwb import NWBHDF5IO

from mindscope_to_nwb_zarr.aind_data_schema.utils import DATA_ASSET_NAME_DATETIME_FORMAT
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.data_description import generate_data_description
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.acquisition import generate_acquisition
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.subject import (
    fetch_subject_from_aind_metadata_service,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.procedures import (
    fetch_procedures_from_aind_metadata_service,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.instrument import generate_instrument

HERE = Path(__file__).resolve().parent
PROJECT_METADATA = HERE.parent.parent / "data" / "visual-behavior-neuropixels" / "project_metadata"
BEHAVIOR_SESSIONS = PROJECT_METADATA / "behavior_sessions.csv"
ECEPHYS_SESSIONS = PROJECT_METADATA / "ecephys_sessions.csv"
# Output goes to the git-ignored code/scratch/ dir (not the tracked scripts/ dir).
OUTPUT_DIR = HERE.parent / "scratch" / "vb_ephys_metadata_all"
REPORT_DIR = OUTPUT_DIR / "_report"
SESSIONS_JSONL = REPORT_DIR / "sessions.jsonl"
SUMMARY_JSON = REPORT_DIR / "summary.json"

# Public S3 bucket (HTTPS endpoint) holding the Visual Behavior Neuropixels NWB files.
S3_BASE_URL = "https://visual-behavior-neuropixels-data.s3.amazonaws.com/visual-behavior-neuropixels"

# A complete session is these five files. subject/procedures require the metadata service,
# so their absence signals an unreachable service, not a data gap; data_description /
# acquisition / instrument are derived from the NWB + session tables and always present.
# With --skip-procedures, procedures.json is dropped from both the generated set and this
# completeness check (see _required_files).
REQUIRED_FILES = ("data_description.json", "subject.json", "acquisition.json",
                  "procedures.json", "instrument.json")


def _required_files(include_procedures: bool) -> tuple:
    """The metadata files a complete session must have, given the procedures toggle."""
    if include_procedures:
        return REQUIRED_FILES
    return tuple(f for f in REQUIRED_FILES if f != "procedures.json")

# The internal AIND metadata service returns empty/truncated bodies under concurrent load,
# and S3 streaming can drop a connection; both are transient, so retry with backoff.
# Streaming is retried on its own; metadata generation is retried WITHOUT re-streaming (the
# open nwbfile stays valid, and a fetch failure happens before any output file is written,
# so a retry is clean).
MAX_STREAM_ATTEMPTS = 3
MAX_GEN_ATTEMPTS = 5


# ---------------------------------------------------------------------------
# Worker (runs in a child process; must be top-level / picklable)
# ---------------------------------------------------------------------------

_BEHAVIOR_CACHE = None
_ECEPHYS_CACHE = None


def _tables():
    """Load the two session tables once per process."""
    global _BEHAVIOR_CACHE, _ECEPHYS_CACHE
    if _BEHAVIOR_CACHE is None:
        _BEHAVIOR_CACHE = pd.read_csv(BEHAVIOR_SESSIONS)
        _ECEPHYS_CACHE = pd.read_csv(ECEPHYS_SESSIONS)
    return _BEHAVIOR_CACHE, _ECEPHYS_CACHE


def ecephys_session_nwb_url(ecephys_session_id: int) -> str:
    """S3 URL for an ecephys session's NWB file."""
    return f"{S3_BASE_URL}/behavior_ecephys_sessions/{ecephys_session_id}/ecephys_session_{ecephys_session_id}.nwb"


def behavior_session_nwb_url(behavior_session_id: int) -> str:
    """S3 URL for a behavior-only session's NWB file."""
    return f"{S3_BASE_URL}/behavior_only_sessions/{behavior_session_id}/behavior_session_{behavior_session_id}.nwb"


def stream_nwb_from_s3(url: str):
    """Stream a session NWB from the public S3 bucket over HTTPS (byte-range reads)."""
    file_handle = remfile.File(url)
    h5_file = h5py.File(file_handle, "r")
    io = NWBHDF5IO(file=h5_file)
    nwbfile = io.read()
    return nwbfile, io, h5_file, file_handle


def _missing_required_files(out_dir: Path | None, required: tuple) -> list:
    """Return the required metadata files not present in out_dir (all of them if None)."""
    if out_dir is None or not out_dir.exists():
        return list(required)
    return [f for f in required if not (out_dir / f).exists()]


def _find_output_dir(subject_id: str, session_start_time) -> Path | None:
    """Locate the folder the generator just wrote for this session.

    The folder is named <subject id>_<acq start>_nwb_<packaging date>; subject id plus
    acquisition start is effectively unique per session, so match on that prefix and take
    the most recently written one.
    """
    prefix = f"{subject_id}_{session_start_time.strftime(DATA_ASSET_NAME_DATETIME_FORMAT)}_nwb_"
    matches = list(OUTPUT_DIR.glob(prefix + "*"))
    if not matches:
        return None
    return max(matches, key=lambda p: p.stat().st_mtime)


def _generate_and_write(nwbfile, session_info: pd.Series, include_procedures: bool = True) -> Path:
    """Run the production generators for one streamed session and write the files.

    Mirrors the production generate_session_metadata (which reads from a local path); here
    the nwbfile is streamed. Returns the output folder holding the written JSON files.
    When ``include_procedures`` is False, procedures.json is not fetched or written.
    """
    # Validate that session description matches metadata (as the production generator does).
    assert nwbfile.session_description == session_info['session_type'], \
        f"Session description mismatch: {nwbfile.session_description} != {session_info['session_type']}"

    data_description = generate_data_description(nwbfile, session_info)
    subject = fetch_subject_from_aind_metadata_service(nwbfile, session_info)
    acquisition = generate_acquisition(nwbfile, session_info)
    procedures = fetch_procedures_from_aind_metadata_service(nwbfile, session_info) if include_procedures else None
    instrument = generate_instrument(session_info)
    models = [data_description, subject, acquisition, procedures, instrument]

    out_dir = OUTPUT_DIR / data_description.name
    out_dir.mkdir(parents=True, exist_ok=True)
    for model in models:
        if model is not None:
            serialized = model.model_dump_json()
            deserialized = model.model_validate_json(serialized)
            deserialized.write_standard_file(output_directory=out_dir)
    return out_dir


def _resolve_session(index: int):
    """Resolve one behavior_sessions row to (session_kind, url, session_info, ids).

    An ecephys session is chosen when the behavior_session_id is present in
    ecephys_sessions.csv; otherwise the session is behavior-only.
    """
    behavior, ecephys = _tables()
    row = behavior.iloc[index]
    behavior_session_id = int(row["behavior_session_id"])

    match = ecephys[ecephys["behavior_session_id"] == behavior_session_id]
    if len(match) > 0:
        session_info = match.iloc[0]
        ecephys_session_id = int(session_info["ecephys_session_id"])
        return "ecephys", ecephys_session_nwb_url(ecephys_session_id), session_info, {
            "behavior_session_id": behavior_session_id,
            "ecephys_session_id": ecephys_session_id,
        }
    return "behavior_only", behavior_session_nwb_url(behavior_session_id), row, {
        "behavior_session_id": behavior_session_id,
        "ecephys_session_id": None,
    }


def process_session(index: int, include_procedures: bool = True) -> dict:
    """Generate metadata for one behavior session (by row index). Never raises.

    Returns a result dict describing status, timing, captured warnings, and (on failure)
    the exception type/message/traceback.
    """
    session_kind, url, session_info, ids = _resolve_session(index)
    behavior_session_id = ids["behavior_session_id"]
    subject_id = str(int(session_info["mouse_id"])) if pd.notna(session_info.get("mouse_id")) else None
    equipment_name = session_info.get("equipment_name")
    required = _required_files(include_procedures)

    result = {
        "index": index,
        "behavior_session_id": behavior_session_id,
        "ecephys_session_id": ids["ecephys_session_id"],
        "subject_id": subject_id,
        "equipment_name": equipment_name if isinstance(equipment_name, str) else None,
        "session_kind": session_kind,
        "session_type": session_info.get("session_type") if isinstance(session_info.get("session_type"), str) else None,
        "include_procedures": include_procedures,
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
            # Stream the NWB (retry on transient connection failures).
            last_exc = None
            for attempt in range(MAX_STREAM_ATTEMPTS):
                caught[:] = []  # keep only the successful attempt's read-phase warnings
                try:
                    nwbfile, io, h5_file, file_handle = stream_nwb_from_s3(url)
                    handles = (io, h5_file, file_handle)
                    break
                except Exception as e:
                    last_exc = e
                    time.sleep(3 * (attempt + 1))
            else:
                raise last_exc

            start_time = nwbfile.session_start_time
            # Snapshot warnings emitted while reading the NWB; the generate loop clears the buffer.
            stream_warnings = list(caught)

            # Generate metadata (retry on transient empty API responses). Clear the
            # captured-warnings buffer before each attempt so only the attempt that
            # ultimately runs contributes warnings.
            last_exc = None
            out_dir = None
            for attempt in range(MAX_GEN_ATTEMPTS):
                caught[:] = []
                result["gen_attempts"] = attempt + 1
                try:
                    out_dir = _generate_and_write(nwbfile, session_info, include_procedures)
                    # Completeness gate: any missing required file (e.g. the metadata
                    # service went dark mid-run) is treated as a transient failure -- delete
                    # the partial folder and retry -- so incomplete output is never recorded OK.
                    missing = _missing_required_files(out_dir, required)
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
                result["n_files"] = len(list(out_dir.glob("*.json")))
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
                for x in handles:
                    try:
                        x.close()
                    except Exception:
                        pass

        # Record captured warnings: read-phase warnings plus the final generate attempt's.
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
    parser.add_argument("--skip-procedures", dest="include_procedures", action="store_false",
                        help="do not fetch/write procedures.json (e.g. while the metadata-service "
                             "procedures endpoint is down); the other files are still generated")
    parser.set_defaults(retry_failed=True, include_procedures=True)
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    behavior = pd.read_csv(BEHAVIOR_SESSIONS)
    total = len(behavior)

    if args.indices is not None:
        indices = args.indices
    else:
        done = load_done_session_ids(args.retry_failed)
        indices = [i for i in range(total) if int(behavior.iloc[i]["behavior_session_id"]) not in done]
        if done:
            print(f"Resuming: {len(done)} sessions already recorded, "
                  f"{len(indices)} pending.", flush=True)
        if args.limit is not None:
            indices = indices[:args.limit]

    print(f"Total sessions: {total} | to process now: {len(indices)} | "
          f"workers: {args.workers}", flush=True)
    print(f"Output:  {OUTPUT_DIR}", flush=True)
    print(f"Report:  {SESSIONS_JSONL}", flush=True)
    if not args.include_procedures:
        print("procedures: SKIPPED (--skip-procedures); procedures.json not generated.", flush=True)
    if not indices:
        print("Nothing to do.", flush=True)
        _write_summary()
        return 0

    n_ok = n_failed = n_warn = 0
    t_start = time.time()
    # Append per-session records as they complete so progress is durable/resumable.
    with open(SESSIONS_JSONL, "a", encoding="utf-8") as jsonl, \
            ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_session, i, args.include_procedures): i for i in indices}
        for n, fut in enumerate(as_completed(futures), 1):
            idx = futures[fut]
            try:
                res = fut.result()
            except Exception as e:  # worker crashed hard (should be rare)
                res = {
                    "index": idx,
                    "behavior_session_id": int(behavior.iloc[idx]["behavior_session_id"]),
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
                detail = f"{res.get('n_files')} files, {res.get('session_kind')}"
            else:
                err = res.get("error") or {}
                detail = f"{err.get('type')}: {err.get('message')}"
            attempts = res.get("gen_attempts")
            retry_note = f" retry#{attempts}" if attempts and attempts > 1 else ""
            print(f"[{n}/{len(indices)}] {marker} bsid {res.get('behavior_session_id')} "
                  f"subj {res.get('subject_id')} {res.get('session_kind')} "
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

    def _by_kind(recs) -> dict:
        out = {}
        for r in recs:
            out[r.get("session_kind")] = out.get(r.get("session_kind"), 0) + 1
        return out

    # Aggregate warnings by (category, normalized message) -> count + example subjects.
    warn_agg = {}
    for r in records:
        for w in r.get("warnings") or []:
            key = (w.get("category"), _normalize(w.get("message", "")))
            entry = warn_agg.setdefault(key, {"category": w.get("category"),
                                              "message_pattern": key[1],
                                              "count": 0, "example_subjects": [],
                                              "session_kinds": []})
            entry["count"] += 1
            if len(entry["example_subjects"]) < 10 and r.get("subject_id") not in entry["example_subjects"]:
                entry["example_subjects"].append(r.get("subject_id"))
            kind = r.get("session_kind")
            if kind not in entry["session_kinds"]:
                entry["session_kinds"].append(kind)

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
        "ok_by_session_kind": _by_kind(ok),
        "failed_by_session_kind": _by_kind(failed),
        "sessions_with_warnings": sum(1 for r in records if r.get("warnings")),
        "total_warnings": sum(len(r.get("warnings") or []) for r in records),
        "warning_types": sorted(warn_agg.values(), key=lambda d: -d["count"]),
        "error_types": sorted(err_agg.values(), key=lambda d: -d["count"]),
        "failed_sessions": [
            {"behavior_session_id": r.get("behavior_session_id"), "subject_id": r.get("subject_id"),
             "session_kind": r.get("session_kind"),
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
    print(f"  OK by kind:     {summary['ok_by_session_kind']}", flush=True)
    print(f"  FAILED by kind: {summary['failed_by_session_kind']}", flush=True)
    if summary["error_types"]:
        print("  Error types:", flush=True)
        for e in summary["error_types"]:
            print(f"    x{e['count']:4d}  {e['type']}: {e['message_pattern'][:100]}", flush=True)
    if summary["warning_types"]:
        print("  Warning types:", flush=True)
        for w in summary["warning_types"]:
            print(f"    x{w['count']:4d}  {w['category']} {w.get('session_kinds')}: "
                  f"{w['message_pattern'][:90]}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
