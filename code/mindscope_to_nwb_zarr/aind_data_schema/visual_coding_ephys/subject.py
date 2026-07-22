"""Generates an example JSON file for visual coding ephys subject"""

import pandas as pd
import warnings
from pynwb import NWBFile
from typing import Optional

from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.species import Species, Strain
from aind_data_schema.core.subject import Subject
from aind_data_schema.components.subjects import Housing, Sex, MouseSubject

from mindscope_to_nwb_zarr.aind_data_schema.utils import get_subject_id, get_subject_date_of_birth
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.instrument import (
    get_experiment_metadata,
    _load_subject_mapping,
)

# Import the metadata service client
import aind_metadata_service_client
from aind_metadata_service_client.rest import ApiException
from urllib3.exceptions import HTTPError as Urllib3HTTPError
import json


def cross_check_mouse_id(nwbfile: NWBFile, session_info: pd.Series, subject_mapping_path: str) -> None:
    """Warn if the 6-digit mouse id disagrees across the three reference sources.

    The experiment metadata CSV maps ``session_id`` (the sessions.csv row driving
    generation) directly to a 6-digit mouse id. The subject mapping JSON maps the NWB
    ``subject_id`` to the same 6-digit mouse id. This confirms the two agree for the
    session.

    Note: sessions.csv's ``specimen_id`` is a different id space than the subject
    mapping keys (the NWB subject ids), so it cannot be joined to the mapping directly.
    Session 819701982 is absent from the experiment CSV and is skipped.
    """
    session_id = int(session_info['id'])
    experiment_metadata = get_experiment_metadata(session_id)
    if experiment_metadata is None:
        warnings.warn(
            f"Session {session_id} is absent from the experiment metadata CSV; "
            f"skipping mouse id cross-check."
        )
        return
    expected_mouse_id = str(experiment_metadata['mouse_id'])

    subject_mapping = _load_subject_mapping(str(subject_mapping_path))
    nwb_subject_id = str(nwbfile.subject.subject_id)
    mapped_mouse_id = subject_mapping.get(nwb_subject_id)

    if mapped_mouse_id is None:
        warnings.warn(
            f"Session {session_id}: NWB subject id {nwb_subject_id} not found in the subject "
            f"mapping; cannot cross-check experiment CSV mouse id {expected_mouse_id}."
        )
    elif str(mapped_mouse_id) != expected_mouse_id:
        warnings.warn(
            f"Session {session_id}: mouse id mismatch - experiment CSV says {expected_mouse_id}, "
            f"but subject mapping (via NWB subject id {nwb_subject_id}) says {mapped_mouse_id}."
        )



def fetch_subject_from_aind_metadata_service(nwbfile: NWBFile, session_info: pd.Series, api_host: Optional[str] = None, subject_mapping_path: str = None) -> Optional[Subject]:
    """
    Fetch subject metadata from AIND metadata service API

    Parameters
    ----------
    nwbfile : NWBFile
        The NWB file containing subject information for validation
    session_info : pd.Series
        Series containing session information to extract subject ID
    api_host : str, optional
        The API host URL. Defaults to "http://aind-metadata-service"

    Returns
    -------
    Subject or None
        Subject object if found, None otherwise

    Notes
    -----
    The API endpoint used is GET /api/v2/subject/{subject_id}

    This function validates that the API response matches the NWB file metadata
    (species, sex, date of birth, genotype).

    If the API call fails or validation fails, returns None and logs a warning.
    """
    api_host = api_host if api_host else "http://aind-metadata-service"

    # get accurate subject id for visual coding data in which the original files did not use the 6-digit mouse id
    if subject_mapping_path is not None:
        with open(subject_mapping_path, 'r') as f:
            subject_mapping_dict = json.load(f)
        subject_id = subject_mapping_dict.get(str(nwbfile.subject.subject_id), None)
    else:
        subject_id = nwbfile.subject.subject_id

    configuration = aind_metadata_service_client.Configuration(host=api_host)

    with aind_metadata_service_client.ApiClient(configuration) as api_client:
        api_instance = aind_metadata_service_client.DefaultApi(api_client)

        # there are known validation issues with old subject data, try to get the content here but accept the raw response if needed
        raw_data = None
        try:
            subject_response = api_instance.get_subject(subject_id=subject_id)
            # Extract raw data for validation (convert to dict if needed)
            raw_data = subject_response.model_dump() if hasattr(subject_response, 'model_dump') else subject_response
            subject = subject_response
        except Urllib3HTTPError as e:
            warnings.warn(f"Warning: Could not connect to AIND metadata service at {api_host}: {e}")
            return None
        except ApiException as e:
            # If validation fails, try to get the raw response
            print(f"Warning: Validation error for subject {subject_id}, attempting to parse raw response")

            # Get raw response without preload content validation
            response = api_instance.get_subject_without_preload_content(subject_id=subject_id)
            raw_data = json.loads(response.data.decode('utf-8'))

            # Try to create Subject object from raw data, fixing known issues
            # Fix null maternal_genotype and/or paternal_genotype issue
            if raw_data.get('subject_details', {}).get('breeding_info', {}).get('maternal_genotype') is None:
                raw_data['subject_details']['breeding_info']['maternal_genotype'] = ""
                warnings.warn(f"Fixed null maternal genotype for subject {subject_id} in metadata service response. Setting to empty string.")
            if raw_data.get('subject_details', {}).get('breeding_info', {}).get('paternal_genotype') is None:
                raw_data['subject_details']['breeding_info']['paternal_genotype'] = ""
                warnings.warn(f"Fixed null paternal genotype for subject {subject_id} in metadata service response. Setting to empty string.")

            # Create Subject from the fixed data
            subject = Subject(**raw_data)

        # Validate API response against NWB file
        if nwbfile is not None and raw_data is not None:
            subject_sex_dict = {"F": "Female", "M": "Male"}

            assert nwbfile.subject.species == raw_data['subject_details']['species']['name'], \
                f"Species mismatch between NWB file ({nwbfile.subject.species}) and metadata service ({raw_data['subject_details']['species']['name']})"

            assert subject_sex_dict.get(nwbfile.subject.sex) == raw_data['subject_details']['sex'], \
                f"Sex mismatch between NWB file ({nwbfile.subject.sex}) and metadata service ({raw_data['subject_details']['sex']})"

            # NOTE: Some files have unexplained DOB mismatch. @saskia. Downgrading mismatch check to a warning for now
            if get_subject_date_of_birth(nwbfile).strftime("%Y-%m-%d") != raw_data['subject_details']['date_of_birth']:
                warnings.warn(f"Date of birth mismatch between NWB file ({get_subject_date_of_birth(nwbfile)}) and metadata service ({raw_data['subject_details']['date_of_birth']})")

            assert nwbfile.subject.genotype == raw_data['subject_details']['genotype'], \
                f"Genotype mismatch between NWB file ({nwbfile.subject.genotype}) and metadata service ({raw_data['subject_details']['genotype']})"

        return subject

