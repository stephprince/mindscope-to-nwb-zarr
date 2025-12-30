"""
Validate that all Visual Coding Ophys experiment metadata rows correspond to
existing DANDI assets.

This script checks that:
1. Each row in the ophys experiment metadata JSON has both a raw and processed
   NWB file on DANDI following the expected naming convention.
2. All asset paths are unique (no duplicates).
3. The total number of NWB files on DANDI matches expectations (2 files per experiment).
"""

from pathlib import Path

from dandi.dandiapi import DandiAPIClient
import pandas as pd
import quilt3 as q3
from tqdm import tqdm

from mindscope_to_nwb_zarr.data_conversion.visual_coding_ophys.run_conversion import (
    DANDISET_ID,
    DANDISET_VERSION,
    S3_BUCKET,
    S3_METADATA_PATH,
    get_dandi_asset_paths,
)

EXPECTED_EXPERIMENT_COUNT = 1518

root_dir = Path(__file__).parent.parent.parent.parent
SCRATCH_DIR = root_dir.parent / "scratch"


def main():
    # Download ophys experiment metadata from S3
    print(f"Downloading ophys experiment metadata from {S3_BUCKET}/{S3_METADATA_PATH} ...")
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    b = q3.Bucket(S3_BUCKET)
    json_path = SCRATCH_DIR / "ophys_experiments.json"
    b.fetch(S3_METADATA_PATH, json_path.as_posix())
    ophys_experiment_metadata = pd.read_json(json_path)
    num_experiments = len(ophys_experiment_metadata)
    print(f"Found {num_experiments} experiments in metadata")

    if num_experiments != EXPECTED_EXPERIMENT_COUNT:
        print(f"WARNING: Expected {EXPECTED_EXPERIMENT_COUNT} experiments, found {num_experiments}")

    # Collect all expected asset paths from metadata
    print("Building expected asset paths from metadata ...")
    expected_processed_paths = set()
    expected_raw_paths = set()
    path_errors = []

    for idx in range(num_experiments):
        row = ophys_experiment_metadata.iloc[idx]
        try:
            processed_path, raw_path = get_dandi_asset_paths(row)
            expected_processed_paths.add(processed_path)
            expected_raw_paths.add(raw_path)
        except ValueError as e:
            path_errors.append(f"Row {idx} (id={row['id']}): {e}")

    if path_errors:
        print(f"\nERROR: {len(path_errors)} rows failed to generate asset paths:")
        for error in path_errors[:10]:
            print(f"  - {error}")
        if len(path_errors) > 10:
            print(f"  ... and {len(path_errors) - 10} more")
        return

    # Check for duplicate paths
    if len(expected_processed_paths) != num_experiments:
        print(f"WARNING: Found duplicate processed paths. Expected {num_experiments}, got {len(expected_processed_paths)}")
    if len(expected_raw_paths) != num_experiments:
        print(f"WARNING: Found duplicate raw paths. Expected {num_experiments}, got {len(expected_raw_paths)}")

    all_expected_paths = expected_processed_paths | expected_raw_paths
    expected_total_files = num_experiments * 2
    print(f"Expected {expected_total_files} total NWB files ({num_experiments} processed + {num_experiments} raw)")

    # Connect to DANDI and get all assets
    print(f"\nConnecting to DANDI dandiset {DANDISET_ID} version {DANDISET_VERSION} ...")
    with DandiAPIClient() as client:
        dandiset = client.get_dandiset(DANDISET_ID, DANDISET_VERSION)

        # Get all asset paths from DANDI
        print("Fetching all asset paths from DANDI (this may take a moment) ...")
        dandi_asset_paths = set()
        for asset in tqdm(dandiset.get_assets(), desc="Fetching assets"):
            dandi_asset_paths.add(asset.path)

        print(f"Found {len(dandi_asset_paths)} total assets on DANDI")

        # Check which expected paths exist
        missing_processed = expected_processed_paths - dandi_asset_paths
        missing_raw = expected_raw_paths - dandi_asset_paths
        found_processed = expected_processed_paths & dandi_asset_paths
        found_raw = expected_raw_paths & dandi_asset_paths

        print(f"\nProcessed files: {len(found_processed)}/{len(expected_processed_paths)} found")
        print(f"Raw files: {len(found_raw)}/{len(expected_raw_paths)} found")

        # Report missing files
        if missing_processed:
            print(f"\nERROR: {len(missing_processed)} processed files not found on DANDI:")
            for path in sorted(missing_processed)[:10]:
                print(f"  - {path}")
            if len(missing_processed) > 10:
                print(f"  ... and {len(missing_processed) - 10} more")

        if missing_raw:
            print(f"\nERROR: {len(missing_raw)} raw files not found on DANDI:")
            for path in sorted(missing_raw)[:10]:
                print(f"  - {path}")
            if len(missing_raw) > 10:
                print(f"  ... and {len(missing_raw) - 10} more")

        # Check for unexpected files on DANDI (files not in our metadata)
        unexpected_files = dandi_asset_paths - all_expected_paths
        if unexpected_files:
            print(f"\nINFO: {len(unexpected_files)} files on DANDI not in experiment metadata:")
            for path in sorted(unexpected_files)[:10]:
                print(f"  - {path}")
            if len(unexpected_files) > 10:
                print(f"  ... and {len(unexpected_files) - 10} more")

        # Final summary
        total_found = len(found_processed) + len(found_raw)
        total_missing = len(missing_processed) + len(missing_raw)

        print("\n" + "=" * 60)
        if total_missing == 0:
            print(f"SUCCESS: All {total_found} expected NWB files found on DANDI")
        else:
            print(f"FAILURE: {total_missing} expected files missing from DANDI")
            print(f"  - {len(missing_processed)} processed files missing")
            print(f"  - {len(missing_raw)} raw files missing")


if __name__ == "__main__":
    main()
