import quilt3 as q3
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
    add_missing_descriptions,
    fix_vector_index_dtypes,
    inspect_zarr_file,
)


def convert_visual_coding_ephys_file_to_zarr(hdf5_base_filename: Path, zarr_filename: Path, probe_filenames: list[Path] = None) -> None:
    """ Convert a Visual Coding Ephys NWB HDF5 file and associated probe files to NWB Zarr format.
    
    The key difference between this and the Visual Behavior Ephys conversion is that the Visual Coding
    dataset does not have stimulus template data included in the NWB files. This data needs to be
    added in separately.
    """

    if probe_filenames is None:
        probe_filenames = []

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

            # TODO - add missing stimulus templates for visual coding dataset
            # load from s3::/allen-brain-observatory/tree/visual-coding-neuropixels/ecephys-cache/natural_scene_templates/
            # load from s3::/allen-brain-observatory/tree/visual-coding-neuropixels/ecephys-cache/natural_movie_templates/
            # load natural movie shuffled from here:
            # load from https://community.brain-map.org/t/accessing-frames-for-natural-movie-shuffled-in-neuropixel-data/1010/22
 
            # add missing experiment description field (from technical white paper)
            nwbfile = add_missing_descriptions(nwbfile)

            # fix VectorIndex dtypes to use minimal unsigned integer types
            nwbfile = fix_vector_index_dtypes(nwbfile)

            # export to zarr
            with NWBZarrIO(zarr_filename, mode='w') as export_io:
                export_io.export(src_io=read_io, nwbfile=nwbfile, write_args=dict(link_data=False))
        finally:
            # close IO objects for probe files
            for probe_io in io_objects:
                probe_io.close()
    
    # inspect and validate the resulting zarr file
    inspect_zarr_file(zarr_filename, 
                      inspector_report_path=zarr_filename.with_suffix('.inspector_report.txt'))
    
if __name__ == "__main__":
    # TODO - this section should be replacable within codeocean with extraction directly from attached data assets

    # get all session ids
    output_dir = Path(".cache/visual_coding_neuropixels_cache_dir")
    b = q3.Bucket("s3://allen-brain-observatory")
    session_metadata_path = "visual-coding-neuropixels/ecephys-cache/sessions.csv"

    session_dir = Path("data/visual_coding_sessions")
    b.fetch(session_metadata_path, session_dir / "sessions.csv")
    session_table = pd.read_csv(session_dir / "sessions.csv")
    session_ids = session_table['id'].astype(str).to_list()

    # download ephys session files
    for session_id in session_ids:
        # get all relevant filenames for that session
        s3_bucket_path = f"visual-coding-neuropixels/ecephys-cache/session_{session_id}/"
        dir_contents = b.ls(s3_bucket_path)[1]
        hdf5_files = [f['Key'] for f in dir_contents if f['IsLatest'] == True and f['Key'].endswith('.nwb')]

        # fetch file from s3 bucket
        local_path = Path(session_dir / f"session_{session_id}")
        local_path.mkdir(parents=True, exist_ok=True)
        for f in hdf5_files:
            if not (local_path / Path(f).name).exists():
                b.fetch(f, local_path / Path(f).name)

        # convert session hdf5_base_filename
        hdf5_base_filename = local_path / f"session_{session_id}.nwb"
        zarr_filename = Path(f"./session_{session_id}.nwb.zarr")
        probe_filenames = [local_path / Path(f).name for f in hdf5_files if 'probe' in f]

        convert_visual_coding_ephys_file_to_zarr(hdf5_base_filename, zarr_filename, probe_filenames)
