import quilt3 as q3

from hdmf_zarr.nwb import NWBZarrIO
from pathlib import Path
from nwbinspector import inspect_nwbfile_object, format_messages, save_report
from pynwb import NWBHDF5IO, load_namespaces
from pynwb.validation import validate
from pynwb.ecephys import ElectricalSeries, LFP

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
        extension_spec = "code/ndx-aibs-ecephys/ndx-aibs-ecephys.namespace.yaml"
        load_namespaces(extension_spec)

        # pull additional data from each of the probe files and add to the main nwbfile
        io_objects = [NWBHDF5IO(f, 'r') for f in probe_filenames]
        for probe_io in io_objects:
            probe_nwbfile = probe_io.read()

            # Build mapping from probe indices to main file indices based on electrode IDs
            probe_electrode_ids = probe_nwbfile.electrodes.id[:]
            main_electrode_ids = nwbfile.electrodes.id[:]

            electrode_mapping = {}
            for old_idx, electrode_id in enumerate(probe_electrode_ids):
                matching_indices = [i for i, main_id in enumerate(main_electrode_ids) if main_id == electrode_id]
                assert len(matching_indices) == 1, f"Expected exactly one matching electrode for ID {electrode_id}, found {len(matching_indices)}"
                electrode_mapping[old_idx] = matching_indices[0]

            acquisition_name = f'probe_{probe_nwbfile.identifier}_lfp'
            lfp_container = probe_nwbfile.acquisition[acquisition_name]
            old_electrical_series = lfp_container[f'{acquisition_name}_data']
            old_electrical_series.reset_parent()

            # Create new electrode table region with updated indices
            old_electrodes = old_electrical_series.electrodes
            new_electrode_indices = [electrode_mapping[idx] for idx in old_electrodes.data]
            new_electrodes_region = nwbfile.create_electrode_table_region(
                region=new_electrode_indices,
                description=old_electrodes.description,
            )

            # Create new LFP container with the updated electrical series
            # WARNING: this is a workaround to modify an attribute that should not be able to be reset, validation should always be performed afterwards
            new_lfp = LFP(name=lfp_container.name, electrical_series=old_electrical_series)
            new_lfp[f'{acquisition_name}_data']._remove_child(new_lfp[f'{acquisition_name}_data'].electrodes)
            new_lfp[f'{acquisition_name}_data'].fields['electrodes'] = new_electrodes_region
            new_lfp[f'{acquisition_name}_data'].fields['electrodes'].parent = new_lfp[f'{acquisition_name}_data']
            
            nwbfile.add_acquisition(new_lfp)

            # Add processing module with general current source density name
            if 'current_source_density' not in nwbfile.processing.keys():                
                nwbfile.create_processing_module(name='current_source_density', 
                                                description=probe_nwbfile.processing['current_source_density'].description)
            csd = probe_nwbfile.processing['current_source_density']['ecephys_csd']
            csd.reset_parent()

            # WARNING: this is a workaround to modify a name, but pynwb does not currently have an API method for this
            csd._AbstractContainer__name = f'probe_{probe_nwbfile.identifier}_ecephys_csd'
            nwbfile.processing['current_source_density'].add(csd)

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
        validate(io=zarr_io)  



# download behavior only session files
# NOTE - these should not require much remapping because they are all contained within a single file
# for session_id in behavior_session_ids:
#     s3_bucket_path = f"visual-behavior-neuropixels/behavior_only_sessions/{session_id}/behavior_session_{session_id}.nwb"
#     b.fetch(s3_bucket_path, f"./behavior_session_{session_id}.nwb")
