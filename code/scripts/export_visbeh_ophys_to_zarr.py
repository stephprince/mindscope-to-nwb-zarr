from pynwb import NWBHDF5IO
from hdmf_zarr.nwb import NWBZarrIO

from pathlib import Path
from nwbinspector import inspect_nwbfile_object, format_messages, save_report

# behavior only
filename = "data/sub-403491_ses-20180824T145125_image.nwb"
zarr_filename = "data/sub-403491_ses-20180824T145125_image.nwb.zarr"

# behavior + ophys
# filename = "data/sub-403491_ses-20181129T093257_image+ophys.nwb"
# zarr_filename = "data/sub-403491_ses-20181129T093257_image+ophys.nwb.zarr"

with NWBHDF5IO(filename, 'r') as read_io:
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
    # validate(io=zarr_io)  