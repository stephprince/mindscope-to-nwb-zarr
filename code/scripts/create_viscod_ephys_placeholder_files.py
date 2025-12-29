"""
Create empty placeholder files for Visual Coding Neuropixels (ephys) sessions.

This script fetches the session list from the Allen Brain Observatory S3 bucket
at s3://allen-brain-observatory/visual-coding-neuropixels/ecephys-cache/sessions.csv
and creates empty placeholder files with the pattern `session_{session_id}.nwb`.

Before creating each placeholder file, the script verifies that the corresponding
NWB file exists in the S3 bucket at the expected path.

These placeholder files are used by the Code Ocean pipeline to determine which
sessions to convert. The actual NWB files are downloaded from S3 during conversion.
"""

from pathlib import Path

import pandas as pd
import quilt3 as q3
from tqdm import tqdm


S3_BUCKET = "s3://allen-brain-observatory"
S3_SESSIONS_CSV_PATH = "visual-coding-neuropixels/ecephys-cache/sessions.csv"
S3_SESSION_DIR_PREFIX = "visual-coding-neuropixels/ecephys-cache"


def main():
    # Output directory for placeholder files
    results_dir = Path(__file__).parent.parent.parent / "data" / "visual-coding-ephys"
    results_dir.mkdir(parents=True, exist_ok=True)

    # Download sessions.csv from S3
    print(f"Fetching sessions.csv from {S3_BUCKET}/{S3_SESSIONS_CSV_PATH}...")
    scratch_dir = Path(__file__).parent.parent.parent / "scratch"
    scratch_dir.mkdir(parents=True, exist_ok=True)

    b = q3.Bucket(S3_BUCKET)
    local_csv_path = scratch_dir / "sessions.csv"
    b.fetch(S3_SESSIONS_CSV_PATH, str(local_csv_path))

    # Read session IDs from CSV
    sessions_df = pd.read_csv(local_csv_path)
    session_ids = sessions_df['id'].tolist()
    expected_count = len(session_ids)

    print(f"Found {expected_count} sessions in sessions.csv")

    # Create placeholder files after verifying S3 file exists
    file_count = 0
    missing_files = []
    for session_id in tqdm(session_ids, desc="Verifying sessions and creating placeholders"):
        filename = f"session_{session_id}.nwb"
        s3_path = f"{S3_SESSION_DIR_PREFIX}/session_{session_id}/{filename}"

        # Check if the file exists in S3
        try:
            # List the directory to verify the file exists
            dir_path = f"{S3_SESSION_DIR_PREFIX}/session_{session_id}/"
            dir_contents = b.ls(dir_path)
            if dir_contents and len(dir_contents) > 1:
                # dir_contents is (prefix, list of objects)
                files_in_dir = [Path(f['Key']).name for f in dir_contents[1] if f.get('IsLatest', True)]
                if filename in files_in_dir:
                    # File exists - create placeholder
                    placeholder_path = results_dir / filename
                    placeholder_path.touch()
                    file_count += 1
                else:
                    missing_files.append(s3_path)
            else:
                missing_files.append(s3_path)
        except Exception as e:
            tqdm.write(f"Error checking {s3_path}: {e}")
            missing_files.append(s3_path)

    print(f"\nCreated {file_count} placeholder files in {results_dir}")

    if missing_files:
        print(f"\nWARNING: {len(missing_files)} session files not found in S3:")
        for f in missing_files[:10]:  # Show first 10
            print(f"  - {f}")
        if len(missing_files) > 10:
            print(f"  ... and {len(missing_files) - 10} more")

    if file_count == expected_count:
        print(f"Success: Created expected {expected_count} files")
    else:
        print(f"Created {file_count} of {expected_count} expected files")


if __name__ == "__main__":
    main()
