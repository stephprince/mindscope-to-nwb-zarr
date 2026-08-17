"""Script to generate AIND data schema JSON files for visual behavior neuropixels dataset"""

import traceback
import pandas as pd

from pathlib import Path
from pynwb import read_nwb

from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.acquisition import generate_acquisition
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.data_description import generate_data_description
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.subject import fetch_subject_from_aind_metadata_service
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.procedures import fetch_procedures_from_aind_metadata_service


def generate_session_metadata(nwb_file_path: Path, session_info: pd.Series, output_dir: Path,
                              include_procedures: bool = True):
    """
    Process a single NWB file and generate AIND data schema JSON files.

    Parameters
    ----------
    nwb_file_path : Path
        Path to the NWB file
    session_info : pd.Series
        Session metadata row from the session table
    output_dir : Path
        Path to directory to save output JSON files
    include_procedures : bool, optional
        Whether to fetch and write procedures.json. Defaults to True. Set to False to skip
        procedures (e.g. while the metadata-service procedures endpoint is unavailable);
        the other models are still generated and procedures can be added on a later run.
    """
    # Read NWB file
    nwbfile = read_nwb(nwb_file_path)

    # Validate that session description matches metadata
    assert nwbfile.session_description == session_info['session_type'], \
        f"Session description mismatch: {nwbfile.session_description} != {session_info['session_type']}"

    # Generate metadata models. subject and procedures are fetched from the AIND metadata
    # service (cached by 6-digit mouse id); both fail loudly on an unexpected outcome
    # (unreachable service, 404, or an NWB/LIMS disagreement) rather than returning None.
    data_description = generate_data_description(nwbfile, session_info)
    subject = fetch_subject_from_aind_metadata_service(nwbfile, session_info)
    acquisition = generate_acquisition(nwbfile, session_info)
    procedures = fetch_procedures_from_aind_metadata_service(nwbfile, session_info) if include_procedures else None
    #instrument = generate_instrument(nwbfile, session_info) # TODO - add instrument generation
    metadata_models = [data_description, subject, acquisition, procedures]  # add instrument when available

    # Save the metadata files
    Path(output_dir / data_description.name).mkdir(parents=True, exist_ok=True)
    for model in metadata_models:
        if model is not None:
            serialized = model.model_dump_json()
            deserialized = model.model_validate_json(serialized)
            deserialized.write_standard_file(output_directory=output_dir / data_description.name)


def generate_all_session_metadata(data_dir: Path, results_dir: Path,
                                  include_procedures: bool = True) -> None:
    """
    Iterate through all sessions in the mounted data directory and generate session metadata.

    The S3 bucket s3://visual-behavior-neuropixels-data is mounted at data_dir/visual-behavior-neuropixels.
    Iterates through all sessions in the behavior_sessions.csv and generates metadata JSON files.

    Parameters
    ----------
    data_dir : Path
        Path to data directory where S3 bucket is mounted
    results_dir : Path
        Path to directory to save output metadata JSON files
    include_procedures : bool, optional
        Whether to fetch and write procedures.json for each session. Defaults to True. Set
        to False to skip procedures while the metadata-service procedures endpoint is
        unavailable; a later run with it True adds procedures.json to each session folder.
    """
    output_dir = results_dir / "visual-behavior-neuropixels-metadata"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Mounted data path
    mounted_data_path = data_dir / "visual-behavior-neuropixels"
    cache_dir = mounted_data_path / "project_metadata"

    # Load session tables
    behavior_sessions_df = pd.read_csv(cache_dir / "behavior_sessions.csv")
    ecephys_sessions_df = pd.read_csv(cache_dir / "ecephys_sessions.csv")

    print(f"Found {len(behavior_sessions_df)} behavior sessions")

    for row_index, behavior_session_row in behavior_sessions_df.iterrows():
        behavior_session_id = int(behavior_session_row['behavior_session_id'])
        print(f"\nProcessing behavior session {behavior_session_id} (row {row_index}) ...")

        # Check if this behavior session has associated ecephys data
        ecephys_match = ecephys_sessions_df[
            ecephys_sessions_df['behavior_session_id'] == behavior_session_id
        ]

        if len(ecephys_match) > 0:
            ecephys_session_id = int(ecephys_match.iloc[0]['ecephys_session_id'])
            session_dir = mounted_data_path / "behavior_ecephys_sessions" / str(ecephys_session_id)
            nwb_filename = f"ecephys_session_{ecephys_session_id}.nwb"
            session_info = ecephys_match.iloc[0]
        else:
            session_dir = mounted_data_path / "behavior_only_sessions" / str(behavior_session_id)
            nwb_filename = f"behavior_session_{behavior_session_id}.nwb"
            session_info = behavior_session_row

        nwb_file_path = session_dir / nwb_filename
        if not nwb_file_path.exists():
            print(f"NWB file not found: {nwb_file_path}. Skipping.")
            continue

        # Generate metadata
        try:
            generate_session_metadata(
                nwb_file_path=nwb_file_path,
                session_info=session_info,
                output_dir=output_dir,
                include_procedures=include_procedures,
            )
        except Exception as e:
            print(f"Error generating metadata for session {behavior_session_id}: {e}")
            traceback.print_exc()
            continue

    print("\nDone generating metadata!")
