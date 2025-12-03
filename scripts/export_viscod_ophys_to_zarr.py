from pynwb import NWBHDF5IO, load_namespaces
from hdmf_zarr.nwb import NWBZarrIO
from hdmf.build import ObjectMapper

from pathlib import Path
from nwbinspector import inspect_nwbfile_object, format_messages, save_report
from pynwb.validation import validate

filename = "data/sub-491604967_ses-496908818-StimB_behavior+image+ophys.nwb"
zarr_filename = "data/sub-491604967_ses-496908818-StimB_behavior+image+ophys.nwb.zarr"

# Load the updated ndx-aibs-ecephys extension into the global type map
# This extension will be used instead of the older extension cached in the NWB file
extension_spec = "ndx-aibs-ecephys/ndx-aibs-ecephys.namespace.yaml"
load_namespaces(extension_spec)

# The NWB extension ndx-aibs-ecephys 0.2.0 specifies a required "strain" text attribute in
# the new data type EcephysSpecimen which extends the NWB core data type Subject.
# However, since the time that the extension was created, the NWB core Subject data type
# has added an optional "strain" dataset. As a result, when reading the NWB file, the
# EcephysSpecimen "strain" field is not populated, leading to a MissingRequiredBuildWarning.
# To work around this, we use a custom ObjectMapper to construct the EcephysSpecimen object
# by getting the "strain" value from the builder "strain" attribute.

# NOTE: The original NWB HDF5 files for Visual Coding - Neuropixels use NWB schema 2.2.0
# where the "filtering" column (VectorData dataset) of the electrodes table is specified
# as a float32 dtype. However, the dataset in the file contains string values. This means
# the original NWB HDF5 file is invalid and the current pynwb validator raises
# this as a validation error. The earlier version of the validator did not catch this 
# validation error. In NWB schema 2.4.0, the "filtering" column was 
# updated to be a variable-length string dtype. When exporting the original NWB file to 
# Zarr using NWBZarrIO and PyNWB 3.1.2 which uses NWB schema 2.9.0, the "filtering" 
# column is read as a string dataset and written to Zarr as a string dataset without error
# or loss of data, so no special handling is needed here.

class CustomEcephysSpecimenMapper(ObjectMapper):

    @ObjectMapper.constructor_arg("strain")
    def strain_carg(self, builder, manager):
        strain_builder = builder.get('strain')
        assert isinstance(strain_builder, str)
        return strain_builder


with NWBHDF5IO(filename, 'r') as read_io:
    # manager = read_io.manager
    # EcephysSpecimen = manager.type_map.get_dt_container_cls('EcephysSpecimen', 'ndx-aibs-ecephys')
    # manager.type_map.register_map(EcephysSpecimen, CustomEcephysSpecimenMapper)

    # Reading the NWB file will cause an expected warning:
    # Ignoring the following cached namespace(s) because another version is already loaded:
    # ndx-aibs-ecephys - cached version: 0.2.0, loaded version: 0.3.0
    # The loaded extension(s) may not be compatible with the cached extension(s) in the file.
    # Please check the extension documentation and ignore this warning if these versions are compatible.
    

    with NWBZarrIO(zarr_filename, mode='w') as export_io:
        export_io.export(src_io=read_io, write_args=dict(link_data=False))

# inspect file for validation errors
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
    # validate(io=nwbfile)  