import pandas as pd

from pathlib import Path
from pynwb import NWBHDF5IO, load_namespaces
from hdmf_zarr.nwb import NWBZarrIO

# load modified extensions before impoting local modules that use them
root_dir = Path(__file__).parent.parent.parent.parent
load_namespaces(str(root_dir / "ndx-aibs-stimulus-template/ndx-aibs-stimulus-template.namespace.yaml"))
load_namespaces(str(root_dir / "ndx-ellipse-eye-tracking/ndx-ellipse-eye-tracking.namespace.yaml"))
load_namespaces(str(root_dir / "ndx-aibs-ecephys/ndx-aibs-ecephys.namespace.yaml"))

from mindscope_to_nwb_zarr.data_conversion.conversion_utils import (
    combine_probe_file_info,
    convert_stimulus_template_to_images,
    add_missing_descriptions,
    inspect_zarr_file,
)

def convert_visual_behavior_ephys_file_to_zarr(hdf5_base_filename: Path, zarr_path: Path, probe_filenames: list[Path] = None) -> None:
    """ Convert a Visual Behavior Ephys NWB HDF5 file and associated probe files to NWB Zarr format."""

    if probe_filenames is None:
        probe_filenames = []

    # Log probe files found
    print(f"\nConverting {hdf5_base_filename.name}")
    print(f"  Found {len(probe_filenames)} probe files:")
    for pf in probe_filenames:
        print(f"    - {pf.name}")

    with NWBHDF5IO(hdf5_base_filename, 'r') as read_io:
        nwbfile = read_io.read()
        nwbfile.subject.strain = "unknown"  # TODO set appropriate strain value
        nwbfile.set_modified()

        # pull additional data from each of the probe files and add to the main nwbfile
        io_objects = [NWBHDF5IO(f, 'r') for f in probe_filenames]
        try:
            for probe_io in io_objects:
                probe_nwbfile = probe_io.read()
                nwbfile = combine_probe_file_info(nwbfile, probe_nwbfile)

            # change stimulus_template to Image objects in Images container
            nwbfile = convert_stimulus_template_to_images(nwbfile)

            # add missing experiment description field (from technical white paper)
            nwbfile = add_missing_descriptions(nwbfile)

            # export to zarr
            with NWBZarrIO(zarr_path, mode='w') as export_io:
                export_io.export(src_io=read_io, nwbfile=nwbfile, write_args=dict(link_data=False))
        finally:
            # close IO objects for probe files
            for probe_io in io_objects:
                probe_io.close()
    
    # inspect and validate the resulting zarr file
    inspect_zarr_file(zarr_path, 
                      inspector_report_path=zarr_path.with_suffix('.inspector_report.txt'))

def iterate_visual_behavior_ephys_sessions(data_dir: Path):
    """Iterate through visual behavior ephys metadata and yield NWB file paths.

    NWB files follow naming patterns:
    - Behavior only: behavior_session_{behavior_session_id}.nwb
    - Behavior-ephys: ecephys_session_{ecephys_session_id}.nwb (base file)
                      + probe_{probe_id}.nwb files (one per probe)

    For ephys sessions, yields session_info dict with nwb_path as the base file
    and probe_paths as a list of probe files.
    """
    # Check for ephys sessions metadata
    ephys_csv_path = data_dir / "project_metadata" / "ecephys_sessions.csv"
    behavior_csv_path = data_dir / "project_metadata" / "behavior_sessions.csv"

    # Process ephys sessions if metadata exists
    df = pd.read_csv(ephys_csv_path)
    for _, row in df.iterrows():
        ecephys_session_id = row['ecephys_session_id']
        session_dir = data_dir / 'behavior_ecephys_sessions' / str(ecephys_session_id)

        # Base NWB file
        base_nwb_path = session_dir / f"ecephys_session_{ecephys_session_id}.nwb"

        # Find all probe files for this session
        probe_paths = []
        if session_dir.exists():
            probe_paths = sorted(session_dir.glob("probe_*.nwb"))

        yield {
            'session_id': ecephys_session_id,
            'session_type': 'behavior_ephys',
            'nwb_path': base_nwb_path,
            'probe_paths': probe_paths,
        }

    # Process behavior-only sessions if metadata exists
    df = pd.read_csv(behavior_csv_path)
    for _, row in df.iterrows():
        behavior_session_id = row['behavior_session_id']
        session_dir = data_dir / 'behavior_only_sessions' / str(behavior_session_id)
        nwb_path = session_dir / f"behavior_session_{behavior_session_id}.nwb"

        yield {
            'session_id': behavior_session_id,
            'session_type': 'behavior',
            'nwb_path': nwb_path,
            'probe_paths': [],
        }

if __name__ == "__main__":
    # TODO - this section should be replacable within codeocean with extraction directly from attached data assets

    # get all session ids
    import quilt3 as q3
    output_dir =  Path(".cache/visual_behavior_neuropixels_cache_dir")
    b = q3.Bucket("s3://visual-behavior-neuropixels-data")
    behavior_session_path = f"visual-behavior-neuropixels/project_metadata/behavior_sessions.csv"
    ephys_session_path = f"visual-behavior-neuropixels/project_metadata/ecephys_sessions.csv"

    convert_ephys_sessions = False
    convert_behavior_sessions = True

    if convert_ephys_sessions:
        session_dir = Path(f"data/behavior_ecephys_sessions")
        b.fetch(ephys_session_path, session_dir / "ecephys_sessions.csv")
        ephys_session_table = pd.read_csv(session_dir / "ecephys_sessions.csv")
        ephys_session_ids = ephys_session_table['ecephys_session_id'].astype(str).to_list()

        # download ephys session files
        for session_id in ephys_session_ids:
            print(f"\n{'='*60}")
            print(f"Processing ephys session {session_id}")
            print(f"{'='*60}")

            # get all relevant filenames for that session
            s3_bucket_path = f"visual-behavior-neuropixels/behavior_ecephys_sessions/{session_id}/"
            dir_contents = b.ls(s3_bucket_path)[1]
            hdf5_files = [f['Key'] for f in dir_contents if f['IsLatest'] == True]
            
            # fetch file from s3 bucket
            local_path = Path(session_dir / session_id)
            local_path.mkdir(parents=True, exist_ok=True)
            for f in hdf5_files:
                if not (local_path / Path(f).name).exists():
                    b.fetch(f, local_path / Path(f).name)

            # validate base session file exists
            hdf5_base_filename = local_path / f"ecephys_session_{session_id}.nwb"
            if not hdf5_base_filename.exists():
                raise FileNotFoundError(
                    f"Base session file not found: {hdf5_base_filename.name}. "
                    f"Available files: {[f.name for f in local_path.glob('*.nwb')]}"
                )

            # identify and validate probe files
            probe_filenames = [local_path / Path(f).name for f in hdf5_files if 'probe' in f]
            zarr_path = Path(f"./ecephys_session_{session_id}.nwb.zarr")
            convert_visual_behavior_ephys_file_to_zarr(hdf5_base_filename, zarr_path, probe_filenames)

    if convert_behavior_sessions:
        session_dir = Path(f"data/behavior_only_sessions")
        b.fetch(behavior_session_path, session_dir / "behavior_sessions.csv")
        behavior_session_table = pd.read_csv(session_dir / "behavior_sessions.csv")
        behavior_session_ids = behavior_session_table['behavior_session_id'].sort_values().to_list()

        # download behavior only session files
        for session_id in behavior_session_ids:
            print(f"\n{'='*60}")
            print(f"Processing behavior session {session_id}")
            print(f"{'='*60}")

            s3_bucket_path = f"visual-behavior-neuropixels/behavior_only_sessions/{session_id}/"
            dir_contents = b.ls(s3_bucket_path)[1]
            hdf5_files = [f['Key'] for f in dir_contents if f['IsLatest'] == True]
            
            # validate exactly one file for behavior-only sessions
            if len(hdf5_files) != 1:
                raise ValueError(
                    f"Expected exactly one file for behavior-only session {session_id}, "
                    f"found {len(hdf5_files)} files: {[Path(f).name for f in hdf5_files]}"
                )
            base_filename = hdf5_files[0]

            # fetch file from s3 bucket
            local_path = Path(f"data/behavior_only_sessions/{session_id}/")
            local_path.mkdir(parents=True, exist_ok=True)
            if not (local_path / Path(base_filename).name).exists():
                b.fetch(base_filename, local_path / Path(base_filename).name)

            # convert session (no probe files for behavior-only)
            zarr_path = Path(f"./behavior_session_{session_id}.nwb.zarr")
            convert_visual_behavior_ephys_file_to_zarr(local_path / Path(base_filename).name, zarr_path)
