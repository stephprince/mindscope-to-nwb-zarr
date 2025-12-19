import argparse
from pathlib import Path
import shutil
import pandas as pd
from pynwb import NWBHDF5IO
from hdmf_zarr.nwb import NWBZarrIO
from nwbinspector import inspect_nwbfile_object, format_messages, save_report


print("STARTING CODE OCEAN CAPSULE RUN")

# Define Code Ocean folder paths
code_folder = Path(__file__).parent
data_folder = Path("../data/")
scratch_folder = Path("../scratch/")
results_folder = Path("../results/")

# Define dataset paths
VISBEH_OPHYS_BEHAVIOR_DATA_DIR = data_folder / "visual-behavior-ophys" / "behavior_sessions"
VISBEH_OPHYS_BEHAVIOR_OPHYS_DATA_DIR = data_folder / "visual-behavior-ophys" / "behavior_ophys_experiments"

VISBEH_OPHYS_METADATA_TABLES_DIR = data_folder / "visual-behavior-ophys" / "project_metadata"
assert VISBEH_OPHYS_METADATA_TABLES_DIR.exists(), \
    f"Visual behavior ophys project metadata tables directory does not exist: {VISBEH_OPHYS_METADATA_TABLES_DIR}"


def format_session_datetime(date_str: str) -> str:
    """Convert date_of_acquisition to NWB filename format.

    Input: '2019-05-17 14:46:33.550000+00:00'
    Output: '20190517T144633'
    """
    # Parse the date string - handle timezone and microseconds
    # Remove timezone info and microseconds for parsing
    date_str = date_str.split('+')[0].split('.')[0]
    # Format: YYYYMMDDTHHMMSS
    parts = date_str.replace('-', '').replace(':', '').replace(' ', 'T')
    return parts


def find_nwb_file(data_dir: Path, subject_id: str, session_datetime: str) -> Path | None:
    """Find NWB file matching subject ID and session datetime.

    NWB files follow patterns:

    Visual Behavior 2P:
    - Behavior: sub-{subject_id}_ses-{datetime}_image.nwb
    - Behavior-ophys: sub-{subject_id}_ses-{datetime}_image+ophys.nwb
                   or sub-{subject_id}_ses-{datetime}_obj-{xxx}_image+ophys.nwb
    """
    if not data_dir.exists():
        return None

    # Pattern to match: sub-{subject}_ses-{datetime}[_obj-xxx]_{suffix}.nwb
    pattern = f"sub-{subject_id}_ses-{session_datetime}"

    for nwb_file in data_dir.glob("*.nwb"):
        if nwb_file.name.startswith(pattern):
            return nwb_file

    return None


def iterate_behavior_sessions():
    """Iterate through behavior_session_table.csv and yield NWB file paths."""
    csv_path = VISBEH_OPHYS_METADATA_TABLES_DIR / "behavior_session_table.csv"
    df = pd.read_csv(csv_path)

    for idx, row in df.iterrows():
        subject_id = str(int(row['mouse_id']))
        date_of_acquisition = row['date_of_acquisition']
        ophys_session_id = row['ophys_session_id']

        # Format the session datetime for filename matching
        session_datetime = format_session_datetime(date_of_acquisition)

        # Determine which data directory to search
        if pd.isna(ophys_session_id):
            # No ophys session - look in behavior-only folder
            data_dir = VISBEH_OPHYS_BEHAVIOR_DATA_DIR
            session_type = "behavior"
        else:
            # Has ophys session - look in behavior+ophys folder
            data_dir = VISBEH_OPHYS_BEHAVIOR_OPHYS_DATA_DIR
            session_type = "behavior_ophys"

        # Find the corresponding NWB file
        nwb_path = find_nwb_file(data_dir, subject_id, session_datetime)

        yield {
            'behavior_session_id': row['behavior_session_id'],
            'subject_id': subject_id,
            'session_datetime': session_datetime,
            'session_type': session_type,
            'nwb_path': nwb_path,
            'data_dir': data_dir,
        }


def convert_visual_behavior_2p_nwb_hdf5_to_zarr(hdf5_path: Path, zarr_path: Path):
    """Convert Visual Behavior 2P NWB file to Zarr format."""
    with NWBHDF5IO(str(hdf5_path), 'r') as read_io:
        zarr_path.touch()  # TODO remove test
        # with NWBZarrIO(str(zarr_path), mode='w') as export_io:
        #     export_io.export(src_io=read_io, write_args=dict(link_data=False))
    print(f"Converted {hdf5_path} to Zarr format at {zarr_path}")


# Code Ocean workflow:
# Iterate through behavior_session_table.csv and process each NWB file
# Example usage:
# python code/run_capsule.py --dataset "Visual Behavior 2P"

def run():
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, required=True)
    args = parser.parse_args()
    dataset = args.dataset

    # Ensure results folder is empty
    for file in results_folder.iterdir():
        shutil.rmtree(file)
    print(f"Cleared results folder: {list(results_folder.iterdir())}")

    # Track sessions with missing NWB files
    missing_nwb_errors = []
    missing_nwb_file_path = results_folder / "missing_nwb_files.txt"

    # Iterate through behavior sessions from CSV
    for session_info in iterate_behavior_sessions():
        behavior_session_id = session_info['behavior_session_id']
        subject_id = session_info['subject_id']
        session_datetime = session_info['session_datetime']
        session_type = session_info['session_type']
        input_nwb_path = session_info['nwb_path']
        data_dir = session_info['data_dir']

        print(f"\n--- Processing session {behavior_session_id} ---")
        print(f"Subject: {subject_id}, DateTime: {session_datetime}, Type: {session_type}")

        if input_nwb_path is None:
            error_msg = (f"behavior_session_id: {behavior_session_id}, "
                         f"subject_id: {subject_id}, "
                         f"session_datetime: {session_datetime}, "
                         f"session_type: {session_type}, "
                         f"expected_pattern: sub-{subject_id}_ses-{session_datetime}_*.nwb, "
                         f"search_dir: {data_dir}")
            missing_nwb_errors.append(error_msg)
            print(f"WARNING: NWB file not found for session {behavior_session_id} "
                  f"(sub-{subject_id}_ses-{session_datetime})")
            continue

        print(f"INPUT NWB: {input_nwb_path}")

        # Output Zarr file path is just the input NWB file name with .zarr suffix in results folder
        result_zarr_path = results_folder / input_nwb_path.name.replace(".nwb", ".nwb.zarr")
        print(f"RESULT ZARR: {result_zarr_path}")

        # Convert based on dataset type
        if dataset.lower() == "visual behavior 2p":
            convert_visual_behavior_2p_nwb_hdf5_to_zarr(
                hdf5_path=input_nwb_path,
                zarr_path=result_zarr_path
            )
        else:
            raise ValueError(f"Dataset not recognized: {dataset}")

        inspector_report_path = results_folder / f"{result_zarr_path.stem}_report.txt"
        print(f"INSPECTOR REPORT PATH: {inspector_report_path}")

        # Inspect output Zarr for validation errors
        # with NWBZarrIO(result_zarr_path, mode='r') as zarr_io:
        #     nwbfile = zarr_io.read()

        #     # Inspect nwb file with io object
        #     # NOTE - this does not run pynwb validation, will run that separately
        #     messages = list(inspect_nwbfile_object(nwbfile))

        #     # Format and print messages nicely
        #     if messages:
        #         formatted_messages = format_messages(
        #             messages=messages,
        #             levels=["importance", "file_path"],
        #             reverse=[True, False]
        #         )
        #         save_report(
        #             report_file_path=inspector_report_path,
        #             formatted_messages=formatted_messages,
        #             overwrite=True,
        #         )

            # Validate file with IO object
            # TODO - waiting to fix hdmf-zarr related validation issues before including
            # validate(io=zarr_io)

    # Write missing NWB file errors to results folder
    if missing_nwb_errors:
        with open(missing_nwb_file_path, 'w') as f:
            f.write("Missing NWB Files Report\n")
            f.write("=" * 50 + "\n\n")
            for error in missing_nwb_errors:
                f.write(error + "\n\n")
        print(f"\nWrote {len(missing_nwb_errors)} missing NWB file errors to {missing_nwb_file_path}")


if __name__ == "__main__":
    run()