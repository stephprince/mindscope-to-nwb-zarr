"""Script to generate AIND data schema JSON files for visual coding ephys dataset"""

import pandas as pd

from pathlib import Path
from pynwb import read_nwb

from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.acquisition import generate_acquisition
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.data_description import generate_data_description
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.subject import fetch_subject_from_aind_metadata_service
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.procedures import fetch_procedures_from_aind_metadata_service

# Path to session metadata CSV files from the data folder
SESSIONS_CSV_PATH = "visual-coding-neuropixels/ecephys-cache/sessions.csv"



def load_session_info(session_id: int, data_dir: Path) -> pd.DataFrame:
    """
    Load session metadata from CSV file.

    Parameters
    ----------
    session_id : int
        Session ID
    data_dir : Path
        Path to data directory containing metadata CSV files

    Returns
    -------
    pd.DataFrame
        Session metadata for the specified session
    """
    sessions_table = pd.read_csv(data_dir / SESSIONS_CSV_PATH)
    session_info = sessions_table.query("id == @session_id")

    if len(session_info) == 0:
        raise ValueError(f"Session ID {session_id} not found in sessions.csv")

    return session_info


def generate_session_metadata(nwb_file_path: Path, session_id: int, data_dir: Path, output_dir: Path, subject_mapping_path: Path) -> None:
    """
    Process a single NWB file and generate AIND data schema JSON files.

    Parameters
    ----------
    nwb_file_path : Path
        Path to the NWB file
    session_id : int
        Session ID for naming output files
    data_dir : Path
        Path to data directory containing metadata CSV files
    output_dir : Path
        Path to directory to save output JSON files
    subject_mapping_path : Path
        Path to JSON file containing subject ID mapping
    """
    # Load allen sdk session info
    session_info = load_session_info(session_id, data_dir)

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


def generate_all_session_metadata(data_dir: Path, results_dir: Path) -> None:
    """
    Iterate through all sessions in the mounted data directory and generate session metadata.

    The S3 bucket s3://allen-brain-observatory is mounted at data_dir/allen-brain-observatory.
    Iterates through all sessions in the sessions.csv and generates metadata JSON files.

    Parameters
    ----------
    data_dir : Path
        Path to data directory where S3 bucket is mounted
    results_dir : Path
        Path to directory to save output metadata JSON files
    """
    output_dir = results_dir / "visual-coding-neuropixels-metadata"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Mounted data path
    mounted_data_path = data_dir / "allen-brain-observatory" / "visual-coding-neuropixels" / "ecephys-cache"

    # Load sessions table
    sessions_df = pd.read_csv(mounted_data_path / "sessions.csv")

    # Subject mapping path
    subject_mapping_path = data_dir / "visual_coding_ephys_subject_mapping.json"

    print(f"Found {len(sessions_df)} sessions")

    for row_index, session_row in sessions_df.iterrows():
        session_id = int(session_row['id'])
        print(f"\nProcessing session {session_id} (row {row_index}) ...")

        # Build NWB file path
        session_dir = mounted_data_path / f"session_{session_id}"
        nwb_filename = f"session_{session_id}.nwb"
        nwb_file_path = session_dir / nwb_filename

        if not nwb_file_path.exists():
            print(f"NWB file not found: {nwb_file_path}. Skipping.")
            continue

        # Generate metadata
        try:
            generate_session_metadata(
                nwb_file_path=nwb_file_path,
                session_id=session_id,
                data_dir=data_dir / "allen-brain-observatory",
                output_dir=output_dir,
                subject_mapping_path=subject_mapping_path,
            )
        except Exception as e:
            print(f"Error generating metadata for session {session_id}: {e}")
            continue
        
        break  # TODO - remove after testing

    print("\nDone generating metadata!")


if __name__ == "__main__":
    repo_root = Path(__file__).parent.parent.parent.parent.parent
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
        generate_session_metadata(
            nwb_file_path=nwb_file_path,
            session_id=session_id,
            data_dir=data_dir,
            output_dir=output_dir,
            subject_mapping_path=subject_mapping_path
        )
    print("\nDone!")
