""" example data description """
import pandas as pd

from datetime import datetime, timezone
from pynwb import NWBFile

from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.data_name_patterns import DataLevel, Group
from aind_data_schema_models.licenses import License
from aind_data_schema.components.identifiers import Person
from aind_data_schema.core.data_description import Funding, DataDescription

from mindscope_to_nwb_zarr.aind_data_schema.utils import get_subject_id, build_data_asset_name
from mindscope_to_nwb_zarr.pynwb_utils import get_modalities, get_data_stream_end_time

def generate_data_description(nwbfile: NWBFile, session_info: pd.Series) -> DataDescription:
    """Create the DataDescription object
    our data always contains planar optical physiology and behavior videos
    """
    subject_id = get_subject_id(nwbfile, session_info=session_info)
    # Asset/folder name: <subject id>_<acquisition start>_nwb_<packaging date (now)>.
    name = build_data_asset_name(subject_id, nwbfile.session_start_time, datetime.now())
    return DataDescription(
        license=License.CC_BY_40,
        subject_id=subject_id,
        creation_time=get_data_stream_end_time(nwbfile).replace(tzinfo=timezone.utc),
        name=name,
        tags=[""], # TODO - add if needed
        institution=Organization.AI,
        funding_source=[Funding(funder=Organization.AI)],
        data_level=DataLevel.DERIVED,
        group=Group.OPHYS,
        investigators=[Person(name="Name")], # TODO - where to pull from?
        project_name="Allen Brain Observatory - Visual Behavior Ophys",
        modalities=get_modalities(nwbfile),
    )