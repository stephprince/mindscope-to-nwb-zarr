"""
Create empty placeholder files for Visual Coding 2p ophys sessions from dandiset 000728.

This script fetches the file list from DANDI Archive dandiset 000728 version 0.240827.1809
and creates empty files with the same names for files ending in "_behavior+image+ophys.nwb".
There should be 1518 such files. There is no file hierarchy created; all files are placed
directly in the output directory.
"""

from pathlib import Path

from dandi.dandiapi import DandiAPIClient


DANDISET_ID = "000728"
DANDISET_VERSION = "0.240827.1809"
FILE_SUFFIX = "_behavior+image+ophys.nwb"
EXPECTED_FILE_COUNT = 1518


def main():
    # Output directory for placeholder files
    results_dir = Path(__file__).parent.parent.parent / "data" / "visual-coding-ophys"
    results_dir.mkdir(parents=True, exist_ok=True)

    print(f"Fetching assets from dandiset {DANDISET_ID} version {DANDISET_VERSION}...")

    with DandiAPIClient() as client:
        dandiset = client.get_dandiset(DANDISET_ID, DANDISET_VERSION)

        file_count = 0
        for asset in dandiset.get_assets():
            asset_path = asset.path
            if asset_path.endswith(FILE_SUFFIX):
                # Extract just the filename from the path
                filename = Path(asset_path).name
                placeholder_path = results_dir / filename

                # Create empty file
                placeholder_path.touch()
                file_count += 1

                if file_count % 100 == 0:
                    print(f"Created {file_count} files...")

    print(f"\nCreated {file_count} placeholder files in {results_dir}")

    if file_count != EXPECTED_FILE_COUNT:
        print(f"WARNING: Expected {EXPECTED_FILE_COUNT} files but created {file_count}")
    else:
        print(f"Success: Created expected {EXPECTED_FILE_COUNT} files")


if __name__ == "__main__":
    main()
