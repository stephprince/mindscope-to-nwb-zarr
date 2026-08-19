"""Generates data description metadata from NWB files for visual behavior ephys sessions.

Mirrors the Visual Coding Ephys and Visual Behavior Ophys data-description modules: a
``mindscope`` + linkage-id tag list, ORCID-tagged investigators, and a ``creation_time``
taken from the last data timestamp (no manual timezone fixups -- the Visual Behavior
Neuropixels NWB ``session_start_time`` is the real acquisition time, already UTC-aware,
unlike the Visual Coding Neuropixels packaging dates).
"""

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


# Columns tagged onto the asset when present and non-null in session_info. The row is
# either an ecephys_sessions row (ecephys sessions) or a behavior_sessions row
# (behavior-only sessions); columns absent from a given table are simply skipped.
#
# These are the Allen LIMS linkage IDs (ecephys/behavior session ids) not otherwise captured
# in the metadata -- they live *only* in these tags, and the conversion pipeline reads them to
# resolve which S3 files to fetch. Fields already represented elsewhere are intentionally
# excluded: mouse_id (DataDescription.subject_id / subject.json), sex / genotype
# (subject.json), session_type (== the Acquisition.acquisition_type / nwbfile.session_description),
# and project_code -- the ecephys-only project_code is the constant "NeuropixelVisualBehavior"
# for every session and duplicates DataDescription.project_name, so tagging it adds no
# information and was dropped.
_TAG_COLUMNS = [
    "ecephys_session_id",
    "behavior_session_id",
]


def _format_tag_value(value) -> str:
    """Render a tag value, coercing integer-valued ids to a plain int string.

    Numeric linkage-id columns (ecephys_session_id / behavior_session_id) are read as float
    when the column contains NaNs, so an id would otherwise render with a trailing ".0"
    (``1040871931.0``); such integer-valued floats are coerced to a plain int string. Any
    non-numeric value passes through unchanged.
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
        Session metadata row (ecephys_sessions row for ecephys sessions,
        behavior_sessions row for behavior-only sessions)

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
        group=Group.EPHYS,
        investigators=[
            Person(name="Corbett Bennett", registry=Registry.ORCID, registry_identifier="0009-0001-2847-7754"),
            Person(name="Shawn Olsen", registry=Registry.ORCID, registry_identifier="0000-0002-9568-7057"),
        ],
        project_name="Allen Brain Observatory - Visual Behavior Neuropixels",
        modalities=get_modalities(nwbfile),
    )
