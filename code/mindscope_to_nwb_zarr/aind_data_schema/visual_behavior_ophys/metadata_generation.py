"""Script to generate AIND data schema JSON files for visual behavior ophys dataset"""

import traceback
import pandas as pd

from pathlib import Path
from pynwb import read_nwb

from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.acquisition_behavior_only import (
    generate_acquisition as generate_acquisition_behavior_only
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.acquisition_behavior_ophys import (
    generate_acquisition as generate_acquisition_behavior_ophys
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.data_description import generate_data_description
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.subject import fetch_subject_from_aind_metadata_service
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.procedures import fetch_procedures_from_aind_metadata_service


def generate_behavior_only_session_metadata(nwb_file_path: Path, session_info: pd.Series, output_dir: Path):
    """
    Process a behavior-only NWB file and generate AIND data schema JSON files.

    Parameters
    ----------
    nwb_file_path : Path
        Path to the NWB file
    session_info : pd.Series
        Session metadata row from the behavior session table
    output_dir : Path
        Path to directory to save output JSON files
    """
    nwbfile = read_nwb(nwb_file_path)

    # Generate metadata models
    data_description = generate_data_description(nwbfile, session_info)
    subject = None  # fetch_subject_from_aind_metadata_service(nwbfile, session_info)
    acquisition = generate_acquisition_behavior_only(nwbfile, session_info)
    procedures = None  # fetch_procedures_from_aind_metadata_service(nwbfile, session_info)
    # instrument = generate_instrument(nwbfile, session_info)  # TODO - add instrument generation
    metadata_models = [data_description, subject, acquisition, procedures]

    # Save the metadata files
    Path(output_dir / data_description.name).mkdir(parents=True, exist_ok=True)
    for model in metadata_models:
        if model is not None:
            serialized = model.model_dump_json()
            deserialized = model.model_validate_json(serialized)
            deserialized.write_standard_file(output_directory=output_dir / data_description.name)


def generate_ophys_session_metadata(
    nwb_file_paths: list[Path],
    session_infos: list[pd.Series],
    output_dir: Path
):
    """
    Process behavior+ophys NWB file(s) and generate AIND data schema JSON files.

    For single-plane sessions, nwb_file_paths contains one file.
    For multiplane sessions, nwb_file_paths contains one file per imaging plane.

    Parameters
    ----------
    nwb_file_paths : list[Path]
        List of paths to NWB file(s). One per imaging plane.
    session_infos : list[pd.Series]
        List of session metadata rows from the ophys experiment table, one per NWB file.
    output_dir : Path
        Path to directory to save output JSON files
    """
    # Read all NWB files
    nwbfiles = [read_nwb(path) for path in nwb_file_paths]

    # Use first file for shared metadata
    nwbfile = nwbfiles[0]
    session_info = session_infos[0]

    # Generate metadata models
    data_description = generate_data_description(nwbfile, session_info)
    subject = None  # fetch_subject_from_aind_metadata_service(nwbfile, session_info)
    acquisition = generate_acquisition_behavior_ophys(nwbfiles, session_infos)
    procedures = None  # fetch_procedures_from_aind_metadata_service(nwbfile, session_info)
    # instrument = generate_instrument(nwbfile, session_info)  # TODO - add instrument generation
    metadata_models = [data_description, subject, acquisition, procedures]

    # Save the metadata files
    Path(output_dir / data_description.name).mkdir(parents=True, exist_ok=True)
    for model in metadata_models:
        if model is not None:
            serialized = model.model_dump_json()
            deserialized = model.model_validate_json(serialized)
            deserialized.write_standard_file(output_directory=output_dir / data_description.name)


def generate_all_session_metadata(data_dir: Path, results_dir: Path) -> None:
    """
    Iterate through all sessions in the mounted data directory and generate session metadata.

    The S3 bucket s3://visual-behavior-ophys-data is mounted at data_dir/visual-behavior-ophys.
    Iterates through all sessions in the behavior_session_table.csv and generates metadata JSON files.

    Parameters
    ----------
    data_dir : Path
        Path to data directory where S3 bucket is mounted
    results_dir : Path
        Path to directory to save output metadata JSON files
    """
    output_dir = results_dir / "visual-behavior-ophys-metadata"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Mounted data path
    mounted_data_path = data_dir / "visual-behavior-ophys"
    cache_dir = mounted_data_path / "project_metadata"

    # Load session tables
    behavior_session_table = pd.read_csv(cache_dir / "behavior_session_table.csv")
    ophys_experiment_table = pd.read_csv(cache_dir / "ophys_experiment_table.csv")

    print(f"Found {len(behavior_session_table)} behavior sessions")

    for row_index, row in behavior_session_table.iterrows():
        behavior_session_id = int(row['behavior_session_id'])
        print(f"\nProcessing behavior session {behavior_session_id} (row {row_index}) ...")

        try:
            # Determine if this is a behavior-only or behavior+ophys session
            if pd.isna(row['ophys_experiment_id']):
                # Behavior-only session
                nwb_file_path = mounted_data_path / "behavior_sessions" / f"behavior_session_{behavior_session_id}.nwb"
                if not nwb_file_path.exists():
                    print(f"NWB file not found: {nwb_file_path}. Skipping.")
                    continue

                generate_behavior_only_session_metadata(
                    nwb_file_path=nwb_file_path,
                    session_info=row,
                    output_dir=output_dir,
                )
            else:
                # Behavior + ophys session
                # Parse all ophys_experiment_ids from string format "[123, 456, 789]" or single int
                ids_str = str(row['ophys_experiment_id']).strip('[]').strip()
                all_ophys_exp_ids = [int(x.strip()) for x in ids_str.split(',')]

                # Build list of NWB file paths and session infos for each plane
                nwb_file_paths = []
                session_infos = []
                for ophys_experiment_id in all_ophys_exp_ids:
                    nwb_path = mounted_data_path / "behavior_ophys_experiments" / f"behavior_ophys_experiment_{ophys_experiment_id}.nwb"
                    if not nwb_path.exists():
                        print(f"  NWB file not found: {nwb_path}. Skipping plane.")
                        continue

                    # Get session info for this ophys experiment from ophys_experiment_table
                    exp_info = ophys_experiment_table.query("ophys_experiment_id == @ophys_experiment_id")
                    if len(exp_info) != 1:
                        print(f"  Could not find unique entry in ophys_experiment_table for {ophys_experiment_id}. Skipping plane.")
                        continue

                    nwb_file_paths.append(nwb_path)
                    session_infos.append(exp_info.iloc[0])

                if len(nwb_file_paths) == 0:
                    print(f"No valid NWB files found for session. Skipping.")
                    continue

                print(f"  Found {len(nwb_file_paths)} plane(s)")
                generate_ophys_session_metadata(
                    nwb_file_paths=nwb_file_paths,
                    session_infos=session_infos,
                    output_dir=output_dir,
                )

        except Exception as e:
            print(f"Error generating metadata for session {behavior_session_id}: {e}")
            traceback.print_exc()
            continue

    print("\nDone generating metadata!")
