import quilt3 as q3

from pynwb import NWBHDF5IO, load_namespaces
from hdmf_zarr.nwb import NWBZarrIO
from pathlib import Path
from nwbinspector import inspect_nwbfile_object, format_messages, save_report
from pynwb.validation import validate

from allensdk.brain_observatory.behavior.behavior_project_cache import VisualBehaviorNeuropixelsProjectCache


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
    with NWBHDF5IO(hdf5_base_filename, 'r') as read_io:
        nwbfile = read_io.read()
        nwbfile.subject.strain = "unknown"  # TODO set appropriate strain value
        nwbfile.set_modified()

        # use modified ndx-aibs-ecephys extension to read and write files
        extension_spec = "ndx-aibs-ecephys/ndx-aibs-ecephys.namespace.yaml"
        load_namespaces(extension_spec)

        # pull additional data from each of the probe files and add to the main nwbfile
        for f in probe_filenames:
            with NWBHDF5IO(f, 'r') as probe_io:
                probe_nwbfile = probe_io.read()

                # First, get the electrode indices from the probe file and add to main electrodes table
                probe_electrode_ids = probe_nwbfile.electrodes.id[:]

                # # Build mapping from old to new electrode indices
                # electrode_mapping = {}
                # for old_idx, electrode_id in enumerate(probe_electrode_ids):
                #     # Copy each electrode from probe to main nwbfile
                #     electrode_row = {col: probe_nwbfile.electrodes[col][old_idx]
                #                    for col in probe_nwbfile.electrodes.colnames}
                #     nwbfile.add_electrode(**electrode_row)
                #     # Map old index to new index in the combined table
                #     new_idx = len(nwbfile.electrodes) - 1
                #     electrode_mapping[old_idx] = new_idx

                # # Get LFP data and update electrode references
                # lfp_data = probe_nwbfile.acquisition[f'probe_{probe_nwbfile.identifier}_lfp']

                # # Create new electrode table region with updated indices
                # old_electrodes = lfp_data.electrical_series['LFP'].electrodes
                # new_electrode_indices = [electrode_mapping[idx] for idx in old_electrodes.data]
                # new_electrodes_region = nwbfile.create_electrode_table_region(
                #     region=new_electrode_indices,
                #     description=old_electrodes.description,
                #     name='electrodes'
                # )

                # # Update the electrical series with new electrode references
                # lfp_data.electrical_series['LFP'].electrodes = new_electrodes_region
                # lfp_data.reset_parent()
                # nwbfile.add_acquisition(lfp_data)

                # Add processing module with probe-specific name
                csd = probe_nwbfile.processing['current_source_density']
                csd.reset_parent()
                csd.name = f'probe_{probe_nwbfile.identifier}_current_source_density'
                nwbfile.add_processing_module(csd)

        # export to zarr
        with NWBZarrIO(zarr_filename, mode='w') as export_io:
            export_io.export(src_io=read_io, nwbfile=nwbfile, write_args=dict(link_data=False))

    # validate the file to make sure it was exported correctly
    with NWBZarrIO(zarr_filename, mode='r') as zarr_io:
        nwbfile = zarr_io.read()

        # inspect nwb file with io object
        # NOTE - this does not run pynwb validation, will run that separately
        messages = list(inspect_nwbfile_object(nwbfile))

        # format and print messages nicely
        if messages:
            formatted_messages = format_messages(
                messages=messages,
                levels=["importance", "file_path"],
                reverse=[True, False]
            )
            save_report(report_file_path=f"data/{Path(zarr_filename).stem}_report.txt", 
                        formatted_messages=formatted_messages,
                        overwrite=True)

        # validate file with IO object
        # TODO - waiting to fix hdmf-zarr related validation issues before including
        # validate(io=zarr_io)  



# download behavior only session files
# NOTE - these should not require much remapping because they are all contained within a single file
# for session_id in behavior_session_ids:
#     s3_bucket_path = f"visual-behavior-neuropixels/behavior_only_sessions/{session_id}/behavior_session_{session_id}.nwb"
#     b.fetch(s3_bucket_path, f"./behavior_session_{session_id}.nwb")
