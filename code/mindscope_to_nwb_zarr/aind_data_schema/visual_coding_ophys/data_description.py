"""Generates data description metadata from NWB files for visual coding ophys sessions"""

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
        Session metadata row from the ophys experiment metadata

    Returns
    -------
    DataDescription
        AIND DataDescription data model populated with data from the NWB file
    """
    # Get subject ID from session metadata (external_donor_name is the 6-digit mouse ID)
    subject_id = session_info['specimen']['donor']['external_donor_name']

    return DataDescription(
        license=License.CC_BY_40,
        subject_id=subject_id,
        creation_time=get_data_stream_end_time(nwbfile).replace(tzinfo=timezone.utc),
        tags=[""],  # TODO - add if needed
        institution=Organization.AIBS,
        funding_source=[Funding(funder=Organization.AI,  # TODO - add if needed
                                grant_number="",  # TODO - add if needed
                                fundee=[Person(name="Name")])],  # TODO - add if needed
        data_level=DataLevel.RAW,
        group=Group.OPHYS,
        investigators=[Person(name="Name")],  # TODO - where to pull from?
        project_name="Visual Coding 2p",
        modalities=get_modalities(nwbfile),
        data_summary=(
            "Two-photon calcium imaging data from the Allen Brain Observatory "
            "Visual Coding project. Mice passively viewed a battery of visual "
            "stimuli including drifting gratings, static gratings, natural scenes, "
            "and natural movies. GCaMP6f-expressing neurons were imaged in visual "
            "cortex (VISp, VISl, VISal, VISpm, VISam, VISrl) at multiple cortical "
            "depths. Each experiment includes cellular fluorescence traces, "
            "extracted events, running speed, eye tracking, and stimulus "
            "presentation timing. Data were collected to characterize how visual "
            "information is encoded across the visual cortical hierarchy."
        )
    )
