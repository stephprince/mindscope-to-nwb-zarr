"""Generates subject metadata from NWB files for visual coding ephys sessions"""

import pandas as pd
from pynwb import NWBFile

from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.species import Species, Strain
from aind_data_schema.core.subject import Subject
from aind_data_schema.components.subjects import Housing, Sex, MouseSubject

from mindscope_to_nwb_zarr.aind_data_schema.utils import get_subject_date_of_birth


def generate_subject(nwbfile: NWBFile, session_info: pd.DataFrame) -> Subject:
    """
    Generate a Subject model from an NWB file and session metadata.

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing subject data
    session_info : pd.DataFrame
        Session metadata information

    Returns
    -------
    Subject
        AIND Subject data model populated with data from the NWB file
    """
    subject_sex_dict = {"F": Sex.FEMALE, "M": Sex.MALE}

    return Subject(
        subject_id=nwbfile.subject.subject_id,
        subject_details=MouseSubject(
            species=Species.HOUSE_MOUSE if nwbfile.subject.species == "Mus musculus" else None,
            strain=Strain.UNKNOWN,  # TODO - add if available
            sex=subject_sex_dict.get(nwbfile.subject.sex),
            date_of_birth=get_subject_date_of_birth(nwbfile),
            source=Organization.OTHER, # TODO - add if available
            breeding_info=None, # TODO add if available
            genotype=nwbfile.subject.genotype,
            housing=Housing(
                    home_cage_enrichment=[],  # TODO - add if available
                    cage_id="unknown",  # TODO - add if available
                ),
        ),
        notes=nwbfile.subject.description if nwbfile.subject.description else None,
    )
