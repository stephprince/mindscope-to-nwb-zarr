"""Generates data description metadata from NWB files for visual coding ophys sessions"""

import pandas as pd

from datetime import datetime, timezone
from pynwb import NWBFile

from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.data_name_patterns import DataLevel, Group
from aind_data_schema_models.licenses import License
from aind_data_schema_models.registries import Registry
from aind_data_schema.components.identifiers import Person
from aind_data_schema.core.data_description import Funding, DataDescription

from mindscope_to_nwb_zarr.pynwb_utils import get_modalities, get_data_stream_end_time
from mindscope_to_nwb_zarr.aind_data_schema.utils import build_data_asset_name
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ophys.instrument import (
    extract_ophys_session_id,
)


def generate_data_description(
    nwbfile: NWBFile, session_info: pd.Series
) -> DataDescription:
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

    # Asset/folder name: <subject id>_<acquisition start>_nwb_<packaging date (now)>.
    name = build_data_asset_name(subject_id, nwbfile.session_start_time, datetime.now())

    # Tag the experiment with its Allen Brain Observatory LIMS IDs. The container and
    # ophys experiment IDs are direct columns of ophys_experiments.json (the experiment
    # id equals nwbfile.session_id). The ophys session id is only recoverable from
    # storage_directory for the newer LIMS prod layout, so it is omitted when absent.
    tags = [
        "mindscope",
        f"specimen_id: {session_info['specimen_id']}",
        f"experiment_container_id: {session_info['experiment_container_id']}",
        f"ophys_experiment_id: {session_info['id']}",
        f"stimulus_name: {session_info['stimulus_name']}",
    ]
    ophys_session_id = extract_ophys_session_id(session_info.get("storage_directory"))
    if ophys_session_id is not None:
        tags.append(f"ophys_session_id: {ophys_session_id}")

    return DataDescription(
        license=License.CC_BY_40,
        subject_id=subject_id,
        creation_time=get_data_stream_end_time(nwbfile).replace(tzinfo=timezone.utc),
        name=name,
        tags=tags,
        institution=Organization.AI,
        funding_source=[Funding(funder=Organization.AI)],
        data_level=DataLevel.DERIVED,
        group=Group.OPHYS,
        investigators=[
            Person(name="Saskia de Vries", registry=Registry.ORCID, registry_identifier="0000-0002-3704-3499"),
            Person(name="Jerome Lecoq", registry=Registry.ORCID, registry_identifier="0000-0002-0131-0938"),
            Person(name="Michael Buice", registry=Registry.ORCID, registry_identifier="0000-0002-2196-1498"),
        ],
        project_name="Allen Brain Observatory - Visual Coding Ophys",
        modalities=get_modalities(nwbfile),
    )
