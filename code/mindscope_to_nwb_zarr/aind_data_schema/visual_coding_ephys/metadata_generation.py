"""Script to generate AIND data schema JSON files for visual coding ephys dataset"""

import pandas as pd

from pathlib import Path
from pynwb import read_nwb

from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.acquisition import generate_acquisition
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.data_description import generate_data_description
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.subject import fetch_subject_from_aind_metadata_service
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.procedures import fetch_procedures_from_aind_metadata_service


def load_session_info(session_id: int, cache_dir: Path) -> pd.DataFrame:
    """
    Load session metadata from CSV file.

    Parameters
    ----------
    session_id : int
        Session ID
    cache_dir : Path
        Path to directory containing metadata CSV files

    Returns
    -------
    pd.DataFrame
        Session metadata for the specified session
    """
    sessions_table = pd.read_csv(cache_dir / "sessions.csv")
    session_info = sessions_table.query("id == @session_id")

    if len(session_info) == 0:
        raise ValueError(f"Session ID {session_id} not found in sessions.csv")

    return session_info


def generate_session_metadata(nwb_file_path: Path, session_id: int, cache_dir: Path, output_dir: Path, subject_mapping_path: Path) -> None:
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
    subject_mapping_path : Path
        Path to JSON file containing subject ID mapping
    """
    # Load allen sdk session info
    session_info = load_session_info(session_id, cache_dir)

    # Read NWB file
    nwbfile = read_nwb(nwb_file_path)

    # Validate that session type matches metadata
    assert nwbfile.stimulus_notes == session_info['session_type'].values[0], \
        f"Session type mismatch: {nwbfile.stimulus_notes} != {session_info['session_type'].values[0]}"

    # Generate metadata models
    data_description = generate_data_description(nwbfile, session_info)
    subject = fetch_subject_from_aind_metadata_service(nwbfile, session_info, subject_mapping_path=subject_mapping_path)
    acquisition = generate_acquisition(nwbfile, session_info)
    procedures = fetch_procedures_from_aind_metadata_service(nwbfile, subject_mapping_path=subject_mapping_path)
    #instrument = generate_instrument(nwbfile, session_info) # TODO - add instrument generation
    metadata_models = [data_description, subject, acquisition, procedures]  # add instrument when available
    
    # Save the metadata files
    Path(output_dir / data_description.name).mkdir(parents=True, exist_ok=True)
    for model in metadata_models:
        serialized = model.model_dump_json()
        deserialized = model.model_validate_json(serialized)
        deserialized.write_standard_file(output_directory=output_dir / data_description.name)


if __name__ == "__main__":
    repo_root = Path(__file__).parent.parent.parent.parent.parent
    cache_dir = repo_root / ".cache/visual_coding_ephys_cache_dir/"
    output_dir = repo_root / "data/schema/ephys_visual_coding/"
    output_dir.mkdir(parents=True, exist_ok=True)
    subject_mapping_path = repo_root / "data/visual_coding_ephys_subject_mapping.json"

    # Define sessions to process
    # TODO - create list of all session ids and corresponding nwb file paths
    sessions = [
        (715093703, repo_root / "data/allen-brain-observatory/visual-coding-neuropixels/ecephys-cache/session_715093703/session_715093703.nwb"),
    ]

    # Process each session
    for session_id, nwb_file_path in sessions:
        print(f"\nProcessing session {session_id}...")
        generate_session_metadata(nwb_file_path=nwb_file_path,
                                  session_id=session_id,
                                  cache_dir=cache_dir,
                                  output_dir=output_dir,
                                  subject_mapping_path=subject_mapping_path)

    print("\nDone!")
