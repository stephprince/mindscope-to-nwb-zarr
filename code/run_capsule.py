import argparse
from pathlib import Path
import shutil
from uuid import uuid4

from mindscope_to_nwb_zarr.data_conversion.conversion_utils import inspect_zarr_file

print("STARTING CODE OCEAN CAPSULE RUN")

# Define Code Ocean folder paths
code_folder = Path(__file__).parent
data_folder = Path("../data/")
scratch_dir = Path("../scratch/")


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
        from mindscope_to_nwb_zarr.data_conversion.visual_behavior_ephys.run_conversion import convert_visual_behavior_ephys_hdf5_to_zarr
        result_zarr_path = convert_visual_behavior_ephys_hdf5_to_zarr(results_dir=results_dir, scratch_dir=scratch_dir)
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


if __name__ == "__main__":
    run()
