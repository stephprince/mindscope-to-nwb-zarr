from pynwb import NWBHDF5IO, load_namespaces
from hdmf_zarr.nwb import NWBZarrIO


filename = "data/sub-506940_ses-20200228T111117_image.nwb"
zarr_filename = "data/sub-506940_ses-20200228T111117_image.nwb.zarr"

# NOTE: Unlike the original NWB HDF5 files for Visual Coding - Neuropixels, 
# the original NWB HDF5 files for Visual Behavior - Neuropixels use a 
# modified ndx-aibs-ecephys extension version 0.2.0 where the "strain" attribute of the
# EcephysSpecimen data type has been removed. The NWB file also does not contain "strain"
# metadata as either a dataset or attribute. Here, we will use the updated ndx-aibs-ecephys
# extension version 0.3.0 which makes "strain" a required dataset of EcephysSpecimen and
# hard-code a value for "strain" when exporting to Zarr.

# TODO validate and inspect the exported Zarr to ensure all fields are written correctly
# and result in a valid NWB file.

# Load the updated ndx-aibs-ecephys extension into the global type map
# This extension will be used instead of the older extension cached in the NWB file
extension_spec = "ndx-aibs-ecephys/ndx-aibs-ecephys.namespace.yaml"
load_namespaces(extension_spec)

with NWBHDF5IO(filename, 'r') as read_io:
    # Reading the NWB file will cause an expected warning:
    #   Ignoring the following cached namespace(s) because another version is already loaded:
    #   ndx-aibs-ecephys - cached version: 0.2.0, loaded version: 0.3.0
    #   The loaded extension(s) may not be compatible with the cached extension(s) in the file.
    #   Please check the extension documentation and ignore this warning if these versions are compatible.

    nwbfile = read_io.read()
    nwbfile.subject.strain = "unknown"  # TODO set appropriate strain value
    nwbfile.set_modified()

    with NWBZarrIO(zarr_filename, mode='w') as export_io:
        export_io.export(src_io=read_io, nwbfile=nwbfile, write_args=dict(link_data=False))

# TODO: Investigate why the exported Zarr file is only 4 MB while the original HDF5 file is 50 MB.
# Everything was compressed in the Zarr file.


