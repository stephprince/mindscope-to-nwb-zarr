"""Generates subject metadata from NWB files for visual coding ophys sessions"""

import json
import warnings
import pandas as pd
from pynwb import NWBFile
from typing import Optional

from aind_data_schema.core.subject import Subject

from mindscope_to_nwb_zarr.aind_data_schema.utils import get_subject_date_of_birth

import aind_metadata_service_client
from aind_metadata_service_client.rest import ApiException
from urllib3.exceptions import HTTPError as Urllib3HTTPError


def fetch_subject_from_aind_metadata_service(
    nwbfile: NWBFile,
    session_info: pd.Series,
    api_host: Optional[str] = None,
) -> Optional[Subject]:
    """
    Fetch subject metadata from AIND metadata service API

    Parameters
    ----------
    nwbfile : NWBFile
        The NWB file containing subject information for validation
    session_info : pd.Series
        Series containing session information (from ophys_experiments.json)
    api_host : str, optional
        The API host URL. Defaults to "http://aind-metadata-service"

    Returns
    -------
    Subject or None
        Subject object if found, None otherwise

    Notes
    -----
    The API endpoint used is GET /api/v2/subject/{subject_id}

    The subject_id is extracted from session_info['specimen']['donor']['external_donor_name'].

    This function validates that the API response matches the NWB file metadata
    (species, sex, date of birth, genotype).

    If the API call fails or validation fails, returns None and logs a warning.
    """
    api_host = api_host if api_host else "http://aind-metadata-service"

    # Get subject ID from session metadata (external_donor_name is the 6-digit mouse ID)
    subject_id = session_info['specimen']['donor']['external_donor_name']

    configuration = aind_metadata_service_client.Configuration(host=api_host)

    with aind_metadata_service_client.ApiClient(configuration) as api_client:
        api_instance = aind_metadata_service_client.DefaultApi(api_client)

        raw_data = None
        try:
            subject_response = api_instance.get_subject(subject_id=subject_id)
            raw_data = subject_response.model_dump() if hasattr(subject_response, 'model_dump') else subject_response
            subject = subject_response
        except Urllib3HTTPError as e:
            warnings.warn(f"Warning: Could not connect to AIND metadata service at {api_host}: {e}")
            return None
        except ApiException as e:
            print(f"Warning: Validation error for subject {subject_id}, attempting to parse raw response")

            response = api_instance.get_subject_without_preload_content(subject_id=subject_id)
            raw_data = json.loads(response.data.decode('utf-8'))

            # Fix null genotype issues
            if raw_data.get('subject_details', {}).get('breeding_info', {}).get('maternal_genotype') is None:
                raw_data['subject_details']['breeding_info']['maternal_genotype'] = ""
                warnings.warn(f"Fixed null maternal genotype for subject {subject_id}")
            if raw_data.get('subject_details', {}).get('breeding_info', {}).get('paternal_genotype') is None:
                raw_data['subject_details']['breeding_info']['paternal_genotype'] = ""
                warnings.warn(f"Fixed null paternal genotype for subject {subject_id}")

            subject = Subject(**raw_data)

        # Validate API response against NWB file
        if nwbfile is not None and raw_data is not None:
            subject_sex_dict = {"F": "Female", "M": "Male"}

            assert nwbfile.subject.species == raw_data['subject_details']['species']['name'], \
                f"Species mismatch: NWB={nwbfile.subject.species}, API={raw_data['subject_details']['species']['name']}"

            assert subject_sex_dict.get(nwbfile.subject.sex) == raw_data['subject_details']['sex'], \
                f"Sex mismatch: NWB={nwbfile.subject.sex}, API={raw_data['subject_details']['sex']}"

            assert get_subject_date_of_birth(nwbfile).strftime("%Y-%m-%d") == raw_data['subject_details']['date_of_birth'], \
                f"Date of birth mismatch: NWB={get_subject_date_of_birth(nwbfile)}, API={raw_data['subject_details']['date_of_birth']}"

            assert nwbfile.subject.genotype == raw_data['subject_details']['genotype'], \
                f"Genotype mismatch: NWB={nwbfile.subject.genotype}, API={raw_data['subject_details']['genotype']}"

        return subject
