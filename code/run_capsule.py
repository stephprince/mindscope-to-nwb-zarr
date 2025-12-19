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


def iterate_behavior_sessions():
    """Iterate through behavior_session_table.csv and yield NWB file paths.

    NWB files follow naming patterns:
    - Behavior only: behavior_session_{behavior_session_id}.nwb
    - Behavior-ophys: behavior_ophys_experiment_{ophys_experiment_id}.nwb

    For behavior-ophys sessions, ophys_experiment_id can be a list of multiple IDs,
    so this function yields one entry per ophys_experiment_id.
    """
    csv_path = VISBEH_OPHYS_METADATA_TABLES_DIR / "behavior_session_table.csv"
    df = pd.read_csv(csv_path)

    for idx, row in df.iterrows():
        behavior_session_id = row['behavior_session_id']
        ophys_experiment_id = row['ophys_experiment_id']

        if pd.isna(ophys_experiment_id):
            # No ophys session - behavior only
            data_dir = VISBEH_OPHYS_BEHAVIOR_DATA_DIR
            session_type = "behavior"
            nwb_filename = f"behavior_session_{behavior_session_id}.nwb"
            nwb_path = data_dir / nwb_filename

            yield {
                'behavior_session_id': behavior_session_id,
                'ophys_experiment_id': None,
                'session_type': session_type,
                'expected_filename': nwb_filename,
                'nwb_path': nwb_path if nwb_path.exists() else None,
                'data_dir': data_dir,
            }
        else:
            # Has ophys session - parse the list of ophys_experiment_ids
            data_dir = VISBEH_OPHYS_BEHAVIOR_OPHYS_DATA_DIR
            session_type = "behavior_ophys"

            # ophys_experiment_id is stored as a string like "[123, 456, 789]"
            # Parse by stripping brackets and splitting on commas
            ids_str = ophys_experiment_id.strip('[]').strip()
            if not ids_str:
                print(f"WARNING: behavior_session_id {behavior_session_id} has empty "
                      f"ophys_experiment_id list, skipping")
                continue
            ophys_exp_ids = [int(x.strip()) for x in ids_str.split(',')]

            if len(ophys_exp_ids) > 1:
                print(f"WARNING: behavior_session_id {behavior_session_id} has "
                      f"{len(ophys_exp_ids)} ophys_experiment_ids: {ophys_exp_ids}")

            for ophys_exp_id in ophys_exp_ids:
                nwb_filename = f"behavior_ophys_experiment_{ophys_exp_id}.nwb"
                nwb_path = data_dir / nwb_filename

                yield {
                    'behavior_session_id': behavior_session_id,
                    'ophys_experiment_id': ophys_exp_id,
                    'session_type': session_type,
                    'expected_filename': nwb_filename,
                    'nwb_path': nwb_path if nwb_path.exists() else None,
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
        ophys_experiment_id = session_info['ophys_experiment_id']
        session_type = session_info['session_type']
        expected_filename = session_info['expected_filename']
        input_nwb_path = session_info['nwb_path']
        data_dir = session_info['data_dir']

        if ophys_experiment_id is not None:
            print(f"\n--- Processing ophys_experiment {ophys_experiment_id} "
                  f"(behavior_session {behavior_session_id}) ---")
        else:
            print(f"\n--- Processing behavior_session {behavior_session_id} ---")
        print(f"Type: {session_type}, Expected file: {expected_filename}")

        if input_nwb_path is None:
            error_msg = (f"behavior_session_id: {behavior_session_id}, "
                         f"ophys_experiment_id: {ophys_experiment_id}, "
                         f"session_type: {session_type}, "
                         f"expected_filename: {expected_filename}, "
                         f"search_dir: {data_dir}")
            missing_nwb_errors.append(error_msg)
            print(f"WARNING: NWB file not found: {expected_filename}")
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