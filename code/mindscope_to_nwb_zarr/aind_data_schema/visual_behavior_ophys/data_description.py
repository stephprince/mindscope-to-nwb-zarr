""" example data description """
import pandas as pd

from datetime import timezone
from pynwb import NWBFile

from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.data_name_patterns import DataLevel, Group
from aind_data_schema_models.licenses import License
from aind_data_schema.components.identifiers import Person
from aind_data_schema.core.data_description import Funding, DataDescription

from mindscope_to_nwb_zarr.aind_data_schema.utils import get_subject_id
from mindscope_to_nwb_zarr.pynwb_utils import get_modalities, get_data_stream_end_time

def generate_data_description(nwbfile: NWBFile, session_info: pd.Series) -> DataDescription:
    """Create the DataDescription object
    our data always contains planar optical physiology and behavior videos
    """
    return DataDescription(
        license=License.CC_BY_40,
        subject_id=get_subject_id(nwbfile, session_info=session_info),
        creation_time=get_data_stream_end_time(nwbfile).replace(tzinfo=timezone.utc),
        tags=[""], # TODO - add if needed
        institution=Organization.AIBS,
        funding_source=[Funding(funder=Organization.AI, # TODO - add if needed
                                grant_number="", # TODO - add if needed
                                fundee=[Person(name="Name")])], # TODO - add if needed
        data_level=DataLevel.RAW,
        group=Group.OPHYS, # TODO - add if needed
        investigators=[Person(name="Name")], # TODO - where to pull from?
        project_name="Visual Behavior 2p", # TODO - confirm
        modalities=get_modalities(nwbfile),
        data_summary=(
            "Two-photon calcium imaging data from the Allen Brain Observatory "
            "Visual Behavior project. Mice were trained to perform a visual "
            "change detection task where they learned to lick in response to "
            "changes in natural scene images. GCaMP6-expressing neurons were "
            "imaged in visual cortex (VISp, VISl, VISal, VISpm, VISam) and "
            "higher visual areas across multiple sessions, enabling tracking "
            "of individual neurons over days to weeks. The dataset includes "
            "cellular fluorescence traces, extracted events, running speed, "
            "eye tracking, licking behavior, and stimulus presentation timing. "
            "Data were collected to study how visual representations and "
            "learned behavior are encoded across the visual cortical hierarchy."
        )
    )