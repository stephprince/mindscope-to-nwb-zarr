"""Generates data description metadata from NWB files for visual coding ephys sessions"""

import pandas as pd

from datetime import datetime
from pynwb import NWBFile

from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.data_name_patterns import DataLevel, Group
from aind_data_schema_models.licenses import License
from aind_data_schema_models.registries import Registry
from aind_data_schema.components.identifiers import Person
from aind_data_schema.core.data_description import Funding, DataDescription

from mindscope_to_nwb_zarr.pynwb_utils import get_modalities, get_data_stream_end_time
from mindscope_to_nwb_zarr.aind_data_schema.utils import build_data_asset_name
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.instrument import (
    get_acquisition_start_time,
    get_mouse_id,
)


def generate_data_description(nwbfile: NWBFile, session_info: pd.Series) -> DataDescription:
    """
    Generate a DataDescription model from an NWB file and session metadata.

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing data description information
    session_info : pd.Series
        Session metadata row

    Returns
    -------
    DataDescription
        AIND DataDescription data model populated with data from the NWB file
    """
    subject_id = get_mouse_id(nwbfile)  # 6-digit mouse ID (the NWB subject_id is a 9-digit LIMS id)

    # Corrected acquisition start (Pacific, from the reference CSV; NWB fallback for the
    # CSV-absent session). Reused for the asset name and to re-anchor creation_time.
    acquisition_start_time = get_acquisition_start_time(nwbfile, session_info)

    # Asset/folder name: <subject id>_<acquisition start>_nwb_<packaging date (now)>.
    name = build_data_asset_name(subject_id, acquisition_start_time, datetime.now())

    tags = [
        "mindscope",
        f"session_id: {session_info['id']}",
        f"specimen_id: {session_info['specimen_id']}",
    ]

    return DataDescription(
        license=License.CC_BY_40,
        subject_id=subject_id,
        # creation_time = acquisition end, kept in Pacific for internal consistency with
        # the acquisition datetimes. The NWB session_start_time is a packaging date, so
        # re-anchor the NWB offsets to the corrected start (matches acquisition_end_time).
        creation_time=get_data_stream_end_time(nwbfile, session_start_time=acquisition_start_time),
        name=name,
        tags=tags,
        institution=Organization.AI,
        funding_source=[Funding(funder=Organization.AI)],
        data_level=DataLevel.DERIVED,
        group=Group.EPHYS,
        investigators=[
            Person(name="Josh Siegle", registry=Registry.ORCID, registry_identifier="0000-0002-7736-4844"),
            Person(name="Xiaoxuan Jia", registry=Registry.ORCID, registry_identifier="0000-0001-5484-9331"),
            Person(name="Shawn Olsen", registry=Registry.ORCID, registry_identifier="0000-0002-9568-7057"),
        ],
        project_name="Allen Brain Observatory - Visual Coding Neuropixels",
        modalities=get_modalities(nwbfile),
    )
