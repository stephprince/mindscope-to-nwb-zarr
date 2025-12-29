import argparse
from pathlib import Path
import shutil
from tqdm import tqdm
from uuid import uuid4

from mindscope_to_nwb_zarr.data_conversion.visual_behavior_ephys.run_conversion import (
    convert_visual_behavior_ephys_file_to_zarr,
    iterate_visual_behavior_ephys_sessions
)

from mindscope_to_nwb_zarr.data_conversion.conversion_utils import inspect_zarr_file

print("STARTING CODE OCEAN CAPSULE RUN")

# Define Code Ocean folder paths
code_folder = Path(__file__).parent
data_folder = Path("../data/")
scratch_dir = Path("../scratch/")

VISBEH_EPHYS_BEHAVIOR_DATA_DIR = data_folder / "visual-behavior-neuropixels"


def convert_visual_behavior_ephys(results_dir: Path) -> str:
    """Convert Visual Behavior Ephys NWB files to Zarr format."""
    sessions = list(iterate_visual_behavior_ephys_sessions(data_dir=VISBEH_EPHYS_BEHAVIOR_DATA_DIR))
    sessions = sessions[:2]  # TODO remove limit
    errors = []

    for session_info in tqdm(sessions, desc="Converting NWB to Zarr"):
        session_type = session_info['session_type']
        nwb_path = session_info['nwb_path']
        session_id = session_info['session_id']

        # Handle visual behavior ephys dataset
        probe_paths = session_info.get('probe_paths', [])

        # Output Zarr file path
        if session_type == 'behavior_ephys':  # behavior ephys
            result_zarr_path = results_dir / "visual-behavior-neuropixels" / "behavior_ecephys_sessions" / f"ecephys_session_{session_id}.nwb.zarr"
        elif session_type == 'behavior':  # behavior only
            result_zarr_path = results_dir / "visual-behavior-neuropixels" / "behavior_only_sessions" / f"behavior_session_{session_id}.nwb.zarr"

        result_zarr_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            convert_visual_behavior_ephys_file_to_zarr(
                hdf5_base_filename=nwb_path,
                zarr_path=result_zarr_path,
                probe_filenames=probe_paths
            )
        except Exception as e:
            error_msg = (f"session_type: {session_type}, "
                         f"session_id: {session_id}, "
                         f"nwb_path: {nwb_path}, "
                         f"error: {str(e)}")
            errors.append(error_msg)
            continue

        # inspect resulting file
        inspector_report_path = result_zarr_path.with_suffix('.inspector_report.txt')
        inspect_zarr_file(zarr_path=result_zarr_path, inspector_report_path=inspector_report_path)

    return errors


def run():
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, required=True)
    parser.add_argument("--results_dir", type=str, default="../results/")
    
    args = parser.parse_args()
    dataset = args.dataset
    results_dir = Path(args.results_dir)

    # Ensure results folder is empty
    for item in results_dir.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
    print(f"Cleared results folder: {list(results_dir.iterdir())}")

    # Convert NWB files based on dataset type
    if dataset.lower() == "visual behavior ephys":
        errors = convert_visual_behavior_ephys(results_dir=results_dir)
    elif dataset.lower() == "visual behavior 2p":
        from mindscope_to_nwb_zarr.data_conversion.visual_behavior_ophys.run_conversion import convert_visual_behavior_ophys_hdf5_to_zarr
        result_zarr_path = convert_visual_behavior_ophys_hdf5_to_zarr(results_dir=results_dir, scratch_dir=scratch_dir)
    elif dataset.lower() == "visual coding ephys":
        from mindscope_to_nwb_zarr.data_conversion.visual_coding_ephys.run_conversion import convert_visual_coding_ephys_hdf5_to_zarr
        result_zarr_path = convert_visual_coding_ephys_hdf5_to_zarr(results_dir=results_dir, scratch_dir=scratch_dir)
    elif dataset.lower() == "visual coding 2p":
        from mindscope_to_nwb_zarr.data_conversion.visual_coding_ophys.run_conversion import convert_visual_coding_ophys_hdf5_to_zarr
        result_zarr_path = convert_visual_coding_ophys_hdf5_to_zarr(results_dir=results_dir, scratch_dir=scratch_dir)
    else:
        raise ValueError(f"Unsupported dataset type: {dataset}")

    # Validate and inspect resulting Zarr file
    if result_zarr_path:
        inspector_report_path = result_zarr_path.with_suffix('.inspector_report.txt')
        print(f"Inspecting resulting Zarr file {result_zarr_path} ...")
        inspect_zarr_file(zarr_path=result_zarr_path, inspector_report_path=inspector_report_path)
    else:
        # Apparently Code Ocean requires at least one output file, so we write an empty file
        dummy_output_path = results_dir / f"dummy_output_{uuid4()}.txt"
        print(f"Writing dummy output file {dummy_output_path} ...")
        dummy_output_path.touch()

    # Write conversion errors to results folder
    # conversion_errors_list_path = results_dir / "conversion_errors.txt"
    # if errors:
    #     with open(conversion_errors_list_path, 'w') as f:
    #         f.write("Conversion Errors Report\n")
    #         f.write("=" * 50 + "\n\n")
    #         for error in errors:
    #             f.write(error + "\n\n")
    #     print(f"\nWrote {len(errors)} NWB file errors to {conversion_errors_list_path}")

    #     print("Conversion Errors Report\n")
    #     print("=" * 50 + "\n\n")
    #     for error in errors:
    #         print(error + "\n\n")


if __name__ == "__main__":
    run()
