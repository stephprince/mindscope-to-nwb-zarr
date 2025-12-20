import argparse
from pathlib import Path
import shutil
import warnings
import pandas as pd
from tqdm import tqdm
from pynwb import NWBHDF5IO
from hdmf_zarr.nwb import NWBZarrIO
from nwbinspector import inspect_nwbfile_object, format_messages, save_report

from scripts.export_visbeh_ophys_to_zarr import (
    convert_behavior_or_single_plane_nwb_to_zarr,
    combine_multiplane_nwb_to_zarr,
)

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

    For multi-plane behavior-ophys sessions (multiple ophys_experiment_ids),
    yields a single session_info dict with nwb_path as a list of paths.
    For single-plane sessions, nwb_path is a single Path object.
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
                'nwb_path': nwb_path,
            }
        else:
            # Has ophys session - parse the list of ophys_experiment_ids
            data_dir = VISBEH_OPHYS_BEHAVIOR_OPHYS_DATA_DIR
            session_type = "behavior_ophys"

            # ophys_experiment_id is stored as a string like "[123, 456, 789]"
            # Parse by stripping brackets and splitting on commas
            ids_str = ophys_experiment_id.strip('[]').strip()
            if not ids_str:
                warnings.warn(f"behavior_session_id {behavior_session_id} has empty "
                              f"ophys_experiment_id list, skipping")
                continue
            ophys_exp_ids = [int(x.strip()) for x in ids_str.split(',')]

            if len(ophys_exp_ids) > 1:
                # Multi-plane session: store all NWB paths in a list
                nwb_paths = [
                    data_dir / f"behavior_ophys_experiment_{ophys_exp_id}.nwb"
                    for ophys_exp_id in ophys_exp_ids
                ]
                yield {
                    'behavior_session_id': behavior_session_id,
                    'ophys_experiment_id': ophys_exp_ids,
                    'session_type': session_type,
                    'nwb_path': nwb_paths,
                }
            else:
                # Single-plane session
                ophys_exp_id = ophys_exp_ids[0]
                nwb_filename = f"behavior_ophys_experiment_{ophys_exp_id}.nwb"
                nwb_path = data_dir / nwb_filename

                yield {
                    'behavior_session_id': behavior_session_id,
                    'ophys_experiment_id': ophys_exp_id,
                    'session_type': session_type,
                    'nwb_path': nwb_path,
                }


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
    for item in results_folder.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
    print(f"Cleared results folder: {list(results_folder.iterdir())}")

    # Convert based on dataset type
    if dataset.lower() == "visual behavior 2p":
        pass  # TODO     
    else:
        raise ValueError(f"Dataset not recognized: {dataset}")

    # Track sessions with missing NWB files
    missing_nwb_errors = []
    missing_nwb_file_path = results_folder / "missing_nwb_files.txt"

    # Collect all sessions first to get total count for progress bar
    sessions = list(iterate_behavior_sessions())
    sessions = sessions[70:80]  # TODO remove limit

    # Iterate through sessions
    for session_info in tqdm(sessions, desc="Converting NWB to Zarr"):
        behavior_session_id = session_info['behavior_session_id']
        ophys_experiment_id = session_info['ophys_experiment_id']
        nwb_path = session_info['nwb_path']

        # Handle multi-plane sessions (nwb_path is a list) vs single-plane (nwb_path is a Path)
        nwb_paths = nwb_path if isinstance(nwb_path, list) else [nwb_path]

        # Check for missing NWB files
        missing_paths = [p for p in nwb_paths if not p.exists()]
        if missing_paths:
            for missing_path in missing_paths:
                error_msg = (f"behavior_session_id: {behavior_session_id}, "
                             f"ophys_experiment_id: {ophys_experiment_id}, "
                             f"nwb_path: {missing_path}")
                missing_nwb_errors.append(error_msg)
            continue

        # Process behavior-only or single-plane sessions
        if not isinstance(nwb_path, list):
            # Output Zarr file path mirrors the input path structure under results folder
            # e.g., ../data/visual-behavior-ophys/behavior_sessions/file.nwb
            #    -> ../results/visual-behavior-ophys/behavior_sessions/file.nwb.zarr
            relative_path = nwb_path.relative_to(data_folder)
            result_zarr_path = results_folder / relative_path.parent / (relative_path.name + ".zarr")
            result_zarr_path.parent.mkdir(parents=True, exist_ok=True)
            convert_behavior_or_single_plane_nwb_to_zarr(
                hdf5_path=nwb_path,
                zarr_path=result_zarr_path
            )

        else:  # Multi-plane sessions
            # Output Zarr combines all experiments (imaging planes) for a session into one NWB Zarr file, so use
            # the behavior_session_id to form the output filename
            result_zarr_path = results_folder / "visual-behavior-ophys" / "behavior_sessions" / f"behavior_ophys_session_{behavior_session_id}.nwb.zarr"
            result_zarr_path.parent.mkdir(parents=True, exist_ok=True)
            combine_multiplane_nwb_to_zarr(
                hdf5_paths=nwb_paths,
                zarr_path=result_zarr_path
            )

        # inspector_report_path = results_folder / f"{result_zarr_path.stem}_report.txt"

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