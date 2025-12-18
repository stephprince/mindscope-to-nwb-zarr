import quilt3 as q3

from pathlib import Path
from pynwb import NWBHDF5IO, load_namespaces
from hdmf_zarr.nwb import NWBZarrIO

from allensdk.brain_observatory.behavior.behavior_project_cache import VisualBehaviorNeuropixelsProjectCache

from mindscope_to_nwb_zarr.data_conversion.conversion_utils import (
    combine_probe_file_info,
    convert_stimulus_template_to_images,
    add_missing_descriptions,
    inspect_zarr_file,
)

def convert_visual_behavior_ephys_file_to_zarr(hdf5_base_filename: Path, zarr_filename: Path, probe_filenames: list[Path]) -> None:
    """ Convert a Visual Behavior Ephys NWB HDF5 file and associated probe files to NWB Zarr format."""

    with NWBHDF5IO(hdf5_base_filename, 'r') as read_io:
        nwbfile = read_io.read()
        nwbfile.subject.strain = "unknown"  # TODO set appropriate strain value
        nwbfile.set_modified()

        # use modified ndx-aibs-ecephys extension to read and write files
        extension_spec = "code/ndx-aibs-ecephys/ndx-aibs-ecephys.namespace.yaml"
        load_namespaces(extension_spec)

        # pull additional data from each of the probe files and add to the main nwbfile
        io_objects = [NWBHDF5IO(f, 'r') for f in probe_filenames]
        for probe_io in io_objects:
            probe_nwbfile = probe_io.read()
            nwbfile = combine_probe_file_info(nwbfile, probe_nwbfile)

        # change stimulus_template to Image objects in Images container
        nwbfile = convert_stimulus_template_to_images(nwbfile)

        # add missing experiment description field (from technical white paper)
        nwbfile = add_missing_descriptions(nwbfile)
        
        # export to zarr
        with NWBZarrIO(zarr_filename, mode='w') as export_io:
            export_io.export(src_io=read_io, nwbfile=nwbfile, write_args=dict(link_data=False))

        # close IO objects for probe files
        for probe_io in io_objects:
            probe_io.close()
    
    # inspect and validate the resulting zarr file
    inspect_zarr_file(zarr_filename)


if __name__ == "__main__":
    # TODO - this section should be replacable within codeocean with extraction directly from attached data assets
    # get all session ids
    output_dir =  Path(".cache/visual_behavior_neuropixels_cache_dir")
    cache = VisualBehaviorNeuropixelsProjectCache.from_s3_cache(cache_dir=output_dir)

    ephys_session_table = cache.get_ecephys_session_table()
    ephys_session_ids = ephys_session_table.index.to_list()

    behavior_session_table = cache.get_behavior_session_table()
    behavior_session_ids = behavior_session_table.index.to_list()

    # download ephys session files
    b = q3.Bucket("s3://visual-behavior-neuropixels-data")
    for session_id in ephys_session_ids:
        # get all relevant filenames for that session
        s3_bucket_path = f"visual-behavior-neuropixels/behavior_ecephys_sessions/{session_id}/"
        dir_contents = b.ls(s3_bucket_path)[1]
        hdf5_files = [f['Key'] for f in dir_contents if f['IsLatest'] == True]
        
        # fetch file from s3 bucket
        local_path = Path(f"data/behavior_ecephys_sessions/{session_id}/")
        local_path.mkdir(parents=True, exist_ok=True)
        for f in hdf5_files:
            if not (local_path / Path(f).name).exists():
                b.fetch(f, local_path / Path(f).name)

        # convert session hdf5_base_filename
        hdf5_base_filename = local_path / f"ecephys_session_{session_id}.nwb"
        zarr_filename = f"./ecephys_session_{session_id}.nwb.zarr"
        probe_filenames = [local_path / Path(f).name for f in hdf5_files if 'probe' in f]

        convert_visual_behavior_ephys_file_to_zarr(hdf5_base_filename, zarr_filename, probe_filenames)
        

    # download behavior only session files
    # for session_id in behavior_session_ids:
    #     s3_bucket_path = f"visual-behavior-neuropixels/behavior_only_sessions/{session_id}/behavior_session_{session_id}.nwb"
    #     b.fetch(s3_bucket_path, f"./behavior_session_{session_id}.nwb")
