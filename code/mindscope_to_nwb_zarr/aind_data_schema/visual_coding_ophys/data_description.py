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
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ophys.instrument import (
    extract_ophys_session_id,
)


def generate_data_description(
    nwbfile: NWBFile, session_info: pd.Series, name: str
) -> DataDescription:
    """
    Generate a DataDescription model from an NWB file and session metadata.

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing data description information
    session_info : pd.Series
        Session metadata row from the ophys experiment metadata
    name : str
        Data asset name, the DANDI asset base name (see ``get_dandi_base_name``),
        e.g. "sub-639389525_ses-653123586-StimC2".

    Returns
    -------
    DataDescription
        AIND DataDescription data model populated with data from the NWB file
    """
    # Get subject ID from session metadata (external_donor_name is the 6-digit mouse ID)
    subject_id = session_info['specimen']['donor']['external_donor_name']

    # Tag the experiment with its Allen Brain Observatory LIMS IDs. The container and
    # ophys experiment IDs are direct columns of ophys_experiments.json (the experiment
    # id equals nwbfile.session_id). The ophys session id is only recoverable from
    # storage_directory for the newer LIMS prod layout, so it is omitted when absent.
    tags = [
        "mindscope",
        f"specimen ID: {session_info['specimen_id']}",
        f"experiment container ID: {session_info['experiment_container_id']}",
        f"ophys experiment ID: {session_info['id']}",
        f"stimulus name: {session_info['stimulus_name']}",
    ]
    ophys_session_id = extract_ophys_session_id(session_info.get("storage_directory"))
    if ophys_session_id is not None:
        tags.append(f"ophys session ID: {ophys_session_id}")

    return DataDescription(
        license=License.CC_BY_40,
        subject_id=subject_id,
        creation_time=get_data_stream_end_time(nwbfile).replace(tzinfo=timezone.utc),
        name=name,
        tags=tags,
        institution=Organization.AI,
        funding_source=[Funding(
            funder=Organization.AI,
            grant_number="Allen Brain Observatory"
        )],
        data_level=DataLevel.DERIVED,
        group=Group.OPHYS,
        investigators=[
            Person(name="Saskia de Vries"),
            Person(name="Jerome Lecoq"),
            Person(name="Michael Buice"),
        ],
        project_name="Allen Brain Observatory - Visual Coding Ophys",
        modalities=get_modalities(nwbfile),
        data_summary=(
            "Two-photon calcium imaging data from the Allen Brain Observatory "
            "Visual Coding project. Mice passively viewed a battery of visual "
            "stimuli including drifting gratings, static gratings, natural scenes, "
            "natural movies, and locally sparse noise, as well as epochs of "
            "spontaneous activity (mean-luminance gray screen). GCaMP6f-expressing "
            "neurons were imaged in visual "
            "cortex (VISp, VISl, VISal, VISpm, VISam, VISrl) at multiple cortical "
            "depths. Each experiment includes cellular fluorescence traces, "
            "extracted events, running speed, eye tracking, and stimulus "
            "presentation timing. Data were collected to characterize how visual "
            "information is encoded across the visual cortical hierarchy."
        )
    )
