"""Batch-generate AIND metadata for ALL Visual Coding Neuropixels (ephys) sessions.

Streams every session NWB from the public allen-brain-observatory S3 bucket over HTTPS
(no download / no mount) and writes the full AIND metadata set (data_description /
subject / acquisition / procedures / instrument) into
scratch/vc_ephys_metadata_test/, then records every session's warnings and any error to
a durable JSONL report (_report/sessions.jsonl) plus an aggregated summary.json. This is
the ephys counterpart of scripts/run_all_vc_ophys.py.

The production pipeline (visual_coding_ephys/metadata_generation.py) reads the NWBs from
a mounted S3 bucket on CodeOcean; locally we stream instead and drive the same production
generator + fetch functions.

Design mirrors run_all_vc_ophys.py: a process pool (warnings.catch_warnings is not
thread-safe, but each worker process has isolated warnings state), per-session JSONL
records, resumable on restart, retry-with-backoff on transient service/stream failures,
and a completeness gate that never records partial output as OK.

Use <= 3 workers: the AIND metadata service returns empty bodies under higher concurrency
(every ephys subject hits the slow raw-parse fallback), and each worker also streams a
~2.6 GB NWB and reads its spike times (for the acquisition end time).

Usage
-----
    uv run python scripts/run_all_vc_ephys.py                 # all 58 sessions
    uv run python scripts/run_all_vc_ephys.py --workers 3
    uv run python scripts/run_all_vc_ephys.py --limit 3       # first 3 pending (smoke test)
    uv run python scripts/run_all_vc_ephys.py --sessions 715093703 767871931
    uv run python scripts/run_all_vc_ephys.py --no-retry-failed
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

from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.acquisition import generate_acquisition
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.data_description import generate_data_description
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.instrument import generate_instrument
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.subject import (
    fetch_subject_from_aind_metadata_service,
    cross_check_mouse_id,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.procedures import (
    fetch_procedures_from_aind_metadata_service,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.metadata_generation import (
    SUBJECT_MAPPING_PATH,
)

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE.parent.parent / "data"
SESSIONS_CSV = DATA_DIR / "allen-brain-observatory" / "visual-coding-neuropixels" / "ecephys-cache" / "sessions.csv"
OUTPUT_DIR = HERE.parent / "scratch" / "vc_ephys_metadata_test"
REPORT_DIR = OUTPUT_DIR / "_report"
SESSIONS_JSONL = REPORT_DIR / "sessions.jsonl"
SUMMARY_JSON = REPORT_DIR / "summary.json"

S3_NWB_URL_TEMPLATE = (
    "https://allen-brain-observatory.s3.amazonaws.com/"
    "visual-coding-neuropixels/ecephys-cache/session_{session_id}/session_{session_id}.nwb"
)

REQUIRED_FILES = ("data_description.json", "subject.json", "acquisition.json",
                  "procedures.json", "instrument.json")

MAX_STREAM_ATTEMPTS = 3
MAX_GEN_ATTEMPTS = 5

_ROWS_CACHE = None


def _rows() -> pd.DataFrame:
    """Load sessions.csv once per process."""
    global _ROWS_CACHE
    if _ROWS_CACHE is None:
        _ROWS_CACHE = pd.read_csv(SESSIONS_CSV)
    return _ROWS_CACHE


def _missing_required_files(out_dir: Path | None) -> list:
    if out_dir is None or not out_dir.exists():
        return list(REQUIRED_FILES)
    return [f for f in REQUIRED_FILES if not (out_dir / f).exists()]


def stream_nwb_from_s3(session_id: int):
    """Stream a session NWB from the public allen-brain-observatory S3 bucket over HTTPS."""
    url = S3_NWB_URL_TEMPLATE.format(session_id=session_id)
    file_handle = remfile.File(url)
    h5_file = h5py.File(file_handle, "r")
    io = NWBHDF5IO(file=h5_file)
    nwbfile = io.read()
    return nwbfile, io, h5_file, file_handle


def _generate_and_write(nwbfile, session_info: pd.Series) -> tuple[Path, int]:
    """Run the production generators for one streamed session and write the 5 files.

    Mirrors the production generate_session_metadata (which reads from a local path);
    here the nwbfile is streamed. Returns (output_dir, n_stimulus_epochs).
    """
    assert nwbfile.stimulus_notes == session_info['session_type'], \
        f"Session type mismatch: {nwbfile.stimulus_notes} != {session_info['session_type']}"

    cross_check_mouse_id(nwbfile, session_info, subject_mapping_path=SUBJECT_MAPPING_PATH)

    data_description = generate_data_description(nwbfile, session_info)
    subject = fetch_subject_from_aind_metadata_service(nwbfile, session_info, subject_mapping_path=SUBJECT_MAPPING_PATH)
    acquisition = generate_acquisition(nwbfile, session_info)
    procedures = fetch_procedures_from_aind_metadata_service(nwbfile, subject_mapping_path=SUBJECT_MAPPING_PATH)
    instrument = generate_instrument(session_info)
    models = [data_description, subject, acquisition, procedures, instrument]

    out_dir = OUTPUT_DIR / data_description.name
    out_dir.mkdir(parents=True, exist_ok=True)
    for model in models:
        if model is not None:
            serialized = model.model_dump_json()
            deserialized = model.model_validate_json(serialized)
            deserialized.write_standard_file(output_directory=out_dir)

    n_epochs = len(acquisition.stimulus_epochs) if acquisition is not None else None
    return out_dir, n_epochs


def process_session(session_id: int) -> dict:
    """Generate metadata for one session. Never raises; returns a result record."""
    rows = _rows()
    row = rows[rows["id"] == session_id]
    session_info = row.iloc[0] if len(row) else pd.Series({"id": session_id})

    result = {
        "session_id": session_id,
        "specimen_id": int(session_info["specimen_id"]) if "specimen_id" in session_info else None,
        "session_type": session_info.get("session_type") if hasattr(session_info, "get") else None,
        "status": None,
        "output_folder": None,
        "n_files": None,
        "n_stimulus_epochs": None,
        "elapsed_s": None,
        "gen_attempts": None,
        "warnings": [],
        "error": None,
    }

    time.sleep(random.uniform(0, 3))
    t0 = time.time()
    handles = None
    stream_warnings = []
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            last_exc = None
            for attempt in range(MAX_STREAM_ATTEMPTS):
                caught[:] = []
                try:
                    nwbfile, io, h5_file, file_handle = stream_nwb_from_s3(session_id)
                    handles = (io, h5_file, file_handle)
                    break
                except Exception as e:
                    last_exc = e
                    time.sleep(3 * (attempt + 1))
            else:
                raise last_exc
            stream_warnings = list(caught)

            last_exc = None
            out_dir = None
            for attempt in range(MAX_GEN_ATTEMPTS):
                caught[:] = []
                result["gen_attempts"] = attempt + 1
                try:
                    out_dir, n_epochs = _generate_and_write(nwbfile, session_info)
                    missing = _missing_required_files(out_dir)
                    if missing:
                        if out_dir is not None and out_dir.exists():
                            shutil.rmtree(out_dir, ignore_errors=True)
                        out_dir = None
                        raise RuntimeError(
                            "incomplete metadata (metadata service unreachable?), "
                            f"missing: {', '.join(missing)}"
                        )
                    result["n_stimulus_epochs"] = n_epochs
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
                for h in handles:
                    try:
                        h.close()
                    except Exception:
                        pass

        for w in stream_warnings + list(caught):
            result["warnings"].append({
                "category": w.category.__name__,
                "message": str(w.message),
                "filename": w.filename,
                "lineno": w.lineno,
            })

    result["elapsed_s"] = round(time.time() - t0, 1)
    return result


def load_done_session_ids(retry_failed: bool) -> set:
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
            if rec.get("session_id") is None:
                continue
            if rec.get("status") == "OK" or (not retry_failed):
                done.add(rec["session_id"])
    return done


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=3, help="parallel workers (default 3)")
    parser.add_argument("--limit", type=int, default=None, help="only first N pending sessions")
    parser.add_argument("--sessions", type=int, nargs="*", default=None, help="explicit session ids")
    parser.add_argument("--no-retry-failed", dest="retry_failed", action="store_false")
    parser.set_defaults(retry_failed=True)
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    sessions_df = pd.read_csv(SESSIONS_CSV)
    all_ids = [int(x) for x in sessions_df["id"].tolist()]

    if args.sessions is not None:
        ids = args.sessions
    else:
        done = load_done_session_ids(args.retry_failed)
        ids = [i for i in all_ids if i not in done]
        if done:
            print(f"Resuming: {len(done)} sessions already recorded, {len(ids)} pending.", flush=True)
        if args.limit is not None:
            ids = ids[:args.limit]

    print(f"Total sessions: {len(all_ids)} | to process now: {len(ids)} | workers: {args.workers}", flush=True)
    print(f"Output:  {OUTPUT_DIR}", flush=True)
    print(f"Report:  {SESSIONS_JSONL}", flush=True)
    if not ids:
        print("Nothing to do.", flush=True)
        _write_summary()
        return 0

    n_ok = n_failed = n_warn = 0
    t_start = time.time()
    with open(SESSIONS_JSONL, "a", encoding="utf-8") as jsonl, \
            ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_session, sid): sid for sid in ids}
        for n, fut in enumerate(as_completed(futures), 1):
            sid = futures[fut]
            try:
                res = fut.result()
            except Exception as e:
                res = {"session_id": sid, "status": "WORKER_CRASH",
                       "error": {"type": type(e).__name__, "message": str(e),
                                 "traceback": traceback.format_exc()}, "warnings": []}
            jsonl.write(json.dumps(res) + "\n")
            jsonl.flush()

            status = res.get("status")
            n_ok += status == "OK"
            n_failed += status != "OK"
            nw = len(res.get("warnings") or [])
            n_warn += nw

            elapsed = time.time() - t_start
            rate = n / elapsed if elapsed else 0
            eta = (len(ids) - n) / rate if rate else 0
            marker = "OK  " if status == "OK" else "FAIL"
            if status == "OK":
                detail = f"{res.get('n_files')} files, {res.get('n_stimulus_epochs')} epochs"
            else:
                err = res.get("error") or {}
                detail = f"{err.get('type')}: {err.get('message')}"
            attempts = res.get("gen_attempts")
            retry_note = f" retry#{attempts}" if attempts and attempts > 1 else ""
            print(f"[{n}/{len(ids)}] {marker} session {res.get('session_id')} "
                  f"({res.get('elapsed_s')}s, {nw} warn{retry_note}) {detail} "
                  f"| ok={n_ok} fail={n_failed} eta={eta/60:.0f}m", flush=True)

    print(f"\nFinished {len(ids)} in {(time.time()-t_start)/60:.1f} min. "
          f"ok={n_ok} fail={n_failed} warnings_captured={n_warn}", flush=True)
    _write_summary()
    return 0


_DIGITS = re.compile(r"\d+")


def _normalize(msg: str) -> str:
    return _DIGITS.sub("#", msg or "")


def _write_summary() -> None:
    if not SESSIONS_JSONL.exists():
        return
    latest = {}
    with open(SESSIONS_JSONL) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            latest[rec.get("session_id")] = rec

    records = list(latest.values())
    ok = [r for r in records if r.get("status") == "OK"]
    failed = [r for r in records if r.get("status") not in ("OK", None)]

    warn_agg = {}
    for r in records:
        for w in r.get("warnings") or []:
            key = (w.get("category"), _normalize(w.get("message", "")))
            entry = warn_agg.setdefault(key, {"category": w.get("category"),
                                              "message_pattern": key[1],
                                              "count": 0, "example_sessions": []})
            entry["count"] += 1
            if len(entry["example_sessions"]) < 10 and r.get("session_id") not in entry["example_sessions"]:
                entry["example_sessions"].append(r.get("session_id"))

    err_agg = {}
    for r in failed:
        e = r.get("error") or {}
        key = (e.get("type"), _normalize(e.get("message", "")))
        entry = err_agg.setdefault(key, {"type": e.get("type"), "message_pattern": key[1],
                                         "count": 0, "example_sessions": []})
        entry["count"] += 1
        if len(entry["example_sessions"]) < 20:
            entry["example_sessions"].append(r.get("session_id"))

    epoch_dist = {}
    for r in ok:
        n = r.get("n_stimulus_epochs")
        epoch_dist[n] = epoch_dist.get(n, 0) + 1

    summary = {
        "total_recorded": len(records),
        "ok": len(ok),
        "failed": len(failed),
        "sessions_with_warnings": sum(1 for r in records if r.get("warnings")),
        "total_warnings": sum(len(r.get("warnings") or []) for r in records),
        "stimulus_epoch_distribution": dict(sorted(epoch_dist.items(), key=lambda kv: (kv[0] is None, kv[0]))),
        "warning_types": sorted(warn_agg.values(), key=lambda d: -d["count"]),
        "error_types": sorted(err_agg.values(), key=lambda d: -d["count"]),
        "failed_sessions": [
            {"session_id": r.get("session_id"),
             "error": (r.get("error") or {}).get("type"),
             "message": (r.get("error") or {}).get("message")}
            for r in failed
        ],
    }
    with open(SUMMARY_JSON, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSummary written to {SUMMARY_JSON}", flush=True)
    print(f"  OK={summary['ok']} FAILED={summary['failed']} "
          f"total_warnings={summary['total_warnings']}", flush=True)
    for e in summary["error_types"]:
        print(f"    x{e['count']:4d}  ERR {e['type']}: {e['message_pattern'][:90]}", flush=True)
    for w in summary["warning_types"]:
        print(f"    x{w['count']:4d}  {w['category']}: {w['message_pattern'][:90]}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
