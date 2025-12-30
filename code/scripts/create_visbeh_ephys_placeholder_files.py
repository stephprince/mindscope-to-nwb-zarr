"""
Create empty placeholder files for Visual Behavior Neuropixels (ephys) sessions.

This script fetches the session lists from the Visual Behavior Neuropixels S3 bucket
at s3://visual-behavior-neuropixels-data/visual-behavior-neuropixels/project_metadata/
and creates empty placeholder files with the patterns:
  - `ecephys_session_{ecephys_session_id}.nwb` for behavior+ephys sessions
  - `behavior_session_{behavior_session_id}.nwb` for behavior-only sessions

Before creating each placeholder file, the script verifies that the corresponding
NWB file exists in the S3 bucket at the expected path.

These placeholder files are used by the Code Ocean pipeline to determine which
sessions to convert. The actual NWB files are downloaded from S3 during conversion.
"""

from pathlib import Path

import pandas as pd
import quilt3 as q3
from tqdm import tqdm


S3_BUCKET = "s3://visual-behavior-neuropixels-data"
S3_DATA_PATH = "visual-behavior-neuropixels"
S3_ECEPHYS_SESSIONS_CSV = f"{S3_DATA_PATH}/project_metadata/ecephys_sessions.csv"
S3_BEHAVIOR_SESSIONS_CSV = f"{S3_DATA_PATH}/project_metadata/behavior_sessions.csv"


def main():
    # Output directory for placeholder files
    results_dir = Path(__file__).parent.parent.parent / "data" / "visual-behavior-ephys-placeholders"
    results_dir.mkdir(parents=True, exist_ok=True)

    # Scratch directory for downloading CSVs
    scratch_dir = Path(__file__).parent.parent.parent / "scratch"
    scratch_dir.mkdir(parents=True, exist_ok=True)

    b = q3.Bucket(S3_BUCKET)

    # Download session CSVs from S3
    print(f"Fetching ecephys_sessions.csv from {S3_BUCKET}/{S3_ECEPHYS_SESSIONS_CSV}...")
    ecephys_csv_path = scratch_dir / "ecephys_sessions.csv"
    b.fetch(S3_ECEPHYS_SESSIONS_CSV, ecephys_csv_path.as_posix())

    print(f"Fetching behavior_sessions.csv from {S3_BUCKET}/{S3_BEHAVIOR_SESSIONS_CSV}...")
    behavior_csv_path = scratch_dir / "behavior_sessions.csv"
    b.fetch(S3_BEHAVIOR_SESSIONS_CSV, behavior_csv_path.as_posix())

    # Read session IDs from CSVs
    ecephys_df = pd.read_csv(ecephys_csv_path)
    ecephys_session_ids = ecephys_df['ecephys_session_id'].tolist()

    # Get behavior session IDs that are associated with ecephys sessions
    # These will NOT have separate behavior-only NWB files
    ecephys_behavior_session_ids = set(ecephys_df['behavior_session_id'].tolist())

    # Filter behavior-only sessions to exclude those that are part of ecephys sessions
    behavior_df = pd.read_csv(behavior_csv_path)
    all_behavior_session_ids = behavior_df['behavior_session_id'].tolist()
    behavior_only_session_ids = [
        sid for sid in all_behavior_session_ids
        if sid not in ecephys_behavior_session_ids
    ]

    total_expected = len(ecephys_session_ids) + len(behavior_only_session_ids)
    print(f"Found {len(ecephys_session_ids)} ecephys sessions and {len(behavior_only_session_ids)} behavior-only sessions")
    print(f"  (Excluded {len(all_behavior_session_ids) - len(behavior_only_session_ids)} behavior sessions that are part of ecephys sessions)")
    print(f"Total: {total_expected} sessions")

    # List all files in the ecephys and behavior directories recursively
    print("Listing ecephys session files from S3 (recursive)...")
    ecephys_dir = f"{S3_DATA_PATH}/behavior_ecephys_sessions/"
    ecephys_dir_contents = b.ls(ecephys_dir, recursive=True)[0]
    ecephys_files_on_s3 = set()
    if ecephys_dir_contents and len(ecephys_dir_contents) > 1:
        for f in ecephys_dir_contents:
            if f.get('IsLatest', True) and f['Key'].endswith('.nwb'):
                ecephys_files_on_s3.add(f['Key'])
    print(f"  Found {len(ecephys_files_on_s3)} NWB files in ecephys sessions")

    print("Listing behavior-only session files from S3 (recursive)...")
    behavior_dir = f"{S3_DATA_PATH}/behavior_only_sessions/"
    behavior_dir_contents = b.ls(behavior_dir, recursive=True)[0]
    behavior_files_on_s3 = set()
    if behavior_dir_contents and len(behavior_dir_contents) > 1:
        for f in behavior_dir_contents:
            if f.get('IsLatest', True) and f['Key'].endswith('.nwb'):
                behavior_files_on_s3.add(f['Key'])
    print(f"  Found {len(behavior_files_on_s3)} NWB files in behavior-only sessions")

    # Create placeholder files for ecephys sessions
    file_count = 0
    missing_files = []

    for session_id in tqdm(ecephys_session_ids, desc="Creating ecephys placeholders"):
        filename = f"ecephys_session_{session_id}.nwb"
        s3_path = f"{S3_DATA_PATH}/behavior_ecephys_sessions/{session_id}/{filename}"

        if s3_path in ecephys_files_on_s3:
            placeholder_path = results_dir / filename
            placeholder_path.touch()
            file_count += 1
        else:
            missing_files.append(s3_path)

    # Create placeholder files for behavior-only sessions
    for session_id in tqdm(behavior_only_session_ids, desc="Creating behavior-only placeholders"):
        filename = f"behavior_session_{session_id}.nwb"
        s3_path = f"{S3_DATA_PATH}/behavior_only_sessions/{session_id}/{filename}"

        if s3_path in behavior_files_on_s3:
            placeholder_path = results_dir / filename
            placeholder_path.touch()
            file_count += 1
        else:
            missing_files.append(s3_path)

    print(f"\nCreated {file_count} placeholder files in {results_dir}")

    if missing_files:
        print(f"\nWARNING: {len(missing_files)} session files not found in S3:")
        for f in missing_files[:10]:  # Show first 10
            print(f"  - {f}")
        if len(missing_files) > 10:
            print(f"  ... and {len(missing_files) - 10} more")

    if file_count == total_expected:
        print(f"Success: Created expected {total_expected} files")
    else:
        print(f"Created {file_count} of {total_expected} expected files")


if __name__ == "__main__":
    main()
