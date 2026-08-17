"""Generates data description metadata from NWB files for visual behavior ophys sessions"""

import pandas as pd

from datetime import datetime
from pynwb import NWBFile

from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.data_name_patterns import DataLevel, Group
from aind_data_schema_models.licenses import License
from aind_data_schema_models.registries import Registry
from aind_data_schema.components.identifiers import Person
from aind_data_schema.core.data_description import Funding, DataDescription

from mindscope_to_nwb_zarr.aind_data_schema.utils import get_subject_id, build_data_asset_name
from mindscope_to_nwb_zarr.pynwb_utils import get_modalities, get_data_stream_end_time


# Columns tagged onto the asset when present in session_info. The row is either a
# behavior_session_table row (behavior-only sessions) or an ophys_experiment_table row
# (ophys sessions); the two tables share most of these, and the ophys-only column
# (ophys_experiment_id) is simply skipped when absent.
#
# These are Allen LIMS linkage IDs / project descriptors not otherwise captured in the
# metadata. Fields already represented elsewhere are intentionally excluded: mouse_id
# (DataDescription.subject_id / subject.json), and targeted_structure and imaging_depth
# (the acquisition's imaging plane config).
_TAG_COLUMNS = [
    "project_code",
    "session_type",
    "ophys_session_id",
    "ophys_container_id",
    "behavior_session_id",
    "ophys_experiment_id",
]


def _format_tag_value(value) -> str:
    """Render a tag value, coercing integer-valued ids to a plain int string.

    Numeric linkage-id columns (e.g. ophys_session_id) are read as float when the column
    contains NaNs (behavior-only rows), so an id would otherwise render with a trailing
    ".0" (``951410079.0``). Strings -- including the list-valued ophys_container_id /
    ophys_experiment_id and session_type / project_code -- pass through unchanged.
    """
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _build_tags(session_info: pd.Series) -> list[str]:
    """Build the asset tags from whichever identifying columns are present and non-null."""
    tags = ["mindscope"]
    for column in _TAG_COLUMNS:
        if column in session_info.index and pd.notna(session_info[column]):
            tags.append(f"{column}: {_format_tag_value(session_info[column])}")
    return tags


def generate_data_description(nwbfile: NWBFile, session_info: pd.Series) -> DataDescription:
    """
    Generate a DataDescription model from an NWB file and session metadata.

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing data description information
    session_info : pd.Series
        Session metadata row (behavior_session_table row for behavior-only sessions,
        ophys_experiment_table row for ophys sessions)

    Returns
    -------
    DataDescription
        AIND DataDescription data model populated with data from the NWB file
    """
    subject_id = get_subject_id(nwbfile, session_info=session_info)

    # Asset/folder name: <subject id>_<acquisition start>_nwb_<packaging date (now)>.
    name = build_data_asset_name(subject_id, nwbfile.session_start_time, datetime.now())

    return DataDescription(
        license=License.CC_BY_40,
        subject_id=subject_id,
        creation_time=get_data_stream_end_time(nwbfile),
        name=name,
        tags=_build_tags(session_info),
        institution=Organization.AI,
        funding_source=[Funding(funder=Organization.AI)],
        data_level=DataLevel.DERIVED,
        group=Group.OPHYS,
        investigators=[
            Person(name="Marina Garrett", registry=Registry.ORCID, registry_identifier="0000-0002-5271-2291"),
            Person(name="Peter Groblewski", registry=Registry.ORCID, registry_identifier="0000-0002-8415-1118"),
            Person(name="Shawn Olsen", registry=Registry.ORCID, registry_identifier="0000-0002-9568-7057"),
        ],
        project_name="Allen Brain Observatory - Visual Behavior Ophys",
        modalities=get_modalities(nwbfile),
    )
