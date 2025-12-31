"""Generates data description metadata from NWB files for visual coding ephys sessions"""

import pandas as pd

from datetime import timezone
from pynwb import NWBFile

from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.data_name_patterns import DataLevel, Group
from aind_data_schema_models.licenses import License
from aind_data_schema.components.identifiers import Person
from aind_data_schema.core.data_description import Funding, DataDescription

from mindscope_to_nwb_zarr.pynwb_utils import get_modalities, get_data_stream_end_time


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
    return DataDescription(
        license=License.CC_BY_40,
        subject_id=nwbfile.subject.subject_id,
        creation_time=get_data_stream_end_time(nwbfile).replace(tzinfo=timezone.utc),
        tags=[""], # TODO - add if needed
        institution=Organization.AIBS,
        funding_source=[Funding(funder=Organization.AI, # TODO - add if needed
                                grant_number="", # TODO - add if needed
                                fundee=[Person(name="Name")])], # TODO - add if needed
        data_level=DataLevel.RAW,
        group=Group.EPHYS,
        investigators=[Person(name="Name")], # TODO - where to pull from?
        project_name="Visual Coding Neuropixels",
        modalities=get_modalities(nwbfile),
        data_summary=("in vivo Neuropixels recordings from the Allen Brain Observatory "
                      "to characterize neural coding in the visual cortex using a diverse "
                      "range of visual stimuli") # TODO - update as needed
    )
