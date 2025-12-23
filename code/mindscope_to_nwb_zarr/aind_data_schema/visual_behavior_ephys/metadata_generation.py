"""Script to generate AIND data schema JSON files for visual behavior neuropixels dataset"""

import warnings
import pandas as pd

from pathlib import Path
from pynwb import read_nwb

from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.acquisition import generate_acquisition
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.data_description import generate_data_description
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.subject import fetch_subject_from_api


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
    subject = fetch_subject_from_api(nwbfile, session_info)
    acquisition = generate_acquisition(nwbfile, session_info)
    #procedures = generate_procedures(nwbfile, session_info) # TODO - add procedures generation
    #instrument = generate_instrument(nwbfile, session_info) # TODO - add instrument generation
    metadata_models = [data_description, subject, acquisition]

    # Save the metadata files
    Path(output_dir / data_description.name).mkdir(parents=True, exist_ok=True)
    for model in metadata_models:
        serialized = model.model_dump_json()
        deserialized = model.model_validate_json(serialized)
        deserialized.write_standard_file(output_directory=output_dir / data_description.name)


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
