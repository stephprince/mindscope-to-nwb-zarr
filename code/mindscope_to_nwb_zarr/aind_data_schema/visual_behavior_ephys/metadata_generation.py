"""Script to generate AIND data schema JSON files for visual behavior neuropixels dataset"""

import warnings
import pandas as pd

from pathlib import Path
from pynwb import read_nwb

from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.acquisition import generate_acquisition
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.data_description import generate_data_description
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.subject import fetch_subject_from_aind_metadata_service
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.procedures import fetch_procedures_from_aind_metadata_service


def load_session_info(session_id: int, cache_dir: Path) -> pd.DataFrame:
    """
    Load session metadata from CSV files.

    Parameters
    ----------
    session_id : int
        Session ID
    cache_dir : Path
        Path to directory containing metadata CSV files

    Returns
    -------
    pd.DataFrame
        Session metadata for the specified subject and session
    """
    ephys_session_table = pd.read_csv(cache_dir / "ecephys_sessions.csv")
    behavior_session_table = pd.read_csv(cache_dir / "behavior_sessions.csv")

    session_info = ephys_session_table.query("ecephys_session_id == @session_id")
    behavior_session_info = behavior_session_table.query("behavior_session_id == @session_id")

    if len(session_info) == 0 and len(behavior_session_info) == 1:
        warnings.warn("Session info only found for behavioral data - defaulting to behavior only session")
        session_info = behavior_session_info

    return session_info


def generate_session_metadata(nwb_file_path: Path, session_id: int, cache_dir: Path, output_dir: Path):
    """
    Process a single NWB file and generate AIND data schema JSON files.

    Parameters
    ----------
    nwb_file_path : Path
        Path to the NWB file
    session_id : int
        Session ID for naming output files
    cache_dir : Path
        Path to directory containing metadata CSV files
    output_dir : Path
        Path to directory to save output JSON files
    """
    # Load allen sdk session info
    session_info = load_session_info(session_id, cache_dir)

    # Read NWB file
    nwbfile = read_nwb(nwb_file_path)

    # Validate that session description matches metadata
    assert nwbfile.session_description == session_info['session_type'].values[0], \
        f"Session description mismatch: {nwbfile.session_description} != {session_info['session_type'].values[0]}"

    # Generate metadata models
    data_description = generate_data_description(nwbfile, session_info)
    subject = fetch_subject_from_aind_metadata_service(nwbfile, session_info)
    acquisition = generate_acquisition(nwbfile, session_info)
    procedures = fetch_procedures_from_aind_metadata_service(nwbfile, session_info)
    #instrument = generate_instrument(nwbfile, session_info) # TODO - add instrument generation
    metadata_models = [data_description, subject, acquisition, procedures]  # add instrument when available

    # Save the metadata files
    Path(output_dir / data_description.name).mkdir(parents=True, exist_ok=True)
    for model in metadata_models:
        serialized = model.model_dump_json()
        deserialized = model.model_validate_json(serialized)
        deserialized.write_standard_file(output_directory=output_dir / data_description.name)


def generate_all_session_metadata(data_dir: Path, results_dir: Path) -> None:
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

    for row_index, session_row in behavior_sessions_df.iterrows():
        behavior_session_id = int(session_row['behavior_session_id'])
        print(f"\nProcessing behavior session {behavior_session_id} (row {row_index}) ...")

        # Check if this behavior session has associated ecephys data
        ecephys_match = ecephys_sessions_df[
            ecephys_sessions_df['behavior_session_id'] == behavior_session_id
        ]

        if len(ecephys_match) > 0:
            ecephys_session_id = int(ecephys_match.iloc[0]['ecephys_session_id'])
            session_dir = mounted_data_path / "behavior_ecephys_sessions" / str(ecephys_session_id)
            nwb_filename = f"ecephys_session_{ecephys_session_id}.nwb"
            session_id = ecephys_session_id
        else:
            session_dir = mounted_data_path / "behavior_only_sessions" / str(behavior_session_id)
            nwb_filename = f"behavior_session_{behavior_session_id}.nwb"
            session_id = behavior_session_id

        nwb_file_path = session_dir / nwb_filename
        if not nwb_file_path.exists():
            print(f"NWB file not found: {nwb_file_path}. Skipping.")
            continue

        # Generate metadata
        try:
            generate_session_metadata(
                nwb_file_path=nwb_file_path,
                session_id=session_id,
                cache_dir=cache_dir,
                output_dir=output_dir,
            )
        except Exception as e:
            print(f"Error generating metadata for session {session_id}: {e}")
            continue

        break  # TODO - uncomment after testing

    print("\nDone generating metadata!")


if __name__ == "__main__":
    repo_root = Path(__file__).parent.parent.parent.parent.parent
    cache_dir = repo_root / ".cache/visual_behavior_neuropixels_cache_dir/visual-behavior-neuropixels-0.5.0/project_metadata/"
    output_dir = repo_root / "data/schema/ephys_visual_behavior/"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Define sessions to process
    # TODO - create list of all session ids and corresponding nwb file paths
    sessions = [
        (1014008383, repo_root / "data/visual_behavior_neuropixels/behavior_session_1014008383.nwb"),
        (1043752325, repo_root / "data/visual_behavior_neuropixels/ecephys_session_1043752325.nwb"),
    ]

    # Process each session
    for session_id, nwb_file_path in sessions:
        print(f"\nProcessing session {session_id}...")
        generate_session_metadata(nwb_file_path=nwb_file_path,
                                  session_id=session_id,
                                  cache_dir=cache_dir,
                                  output_dir=output_dir)

    print("\nDone!")
