"""Generates an example JSON file for visual behavior ephys subject"""

import pandas as pd
from pynwb import NWBFile
from typing import Optional

from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.species import Species, Strain
from aind_data_schema.core.subject import Subject
from aind_data_schema.components.subjects import Housing, Sex, MouseSubject

from mindscope_to_nwb_zarr.aind_data_schema.utils import get_subject_id, get_subject_date_of_birth

# Import the metadata service client
import aind_metadata_service_client
from aind_metadata_service_client.rest import ApiException
import json


def fetch_subject_from_api(nwbfile: NWBFile, session_info: pd.DataFrame, api_host: Optional[str] = None, ) -> Optional[Subject]:
    """
    Fetch subject metadata from AIND metadata service API

    Parameters
    ----------
    subject_id : str
        The subject ID to query
    api_host : str, optional
        The API host URL. Defaults to "http://aind-metadata-service"

    Returns
    -------
    Subject or None
        Subject object if found, None otherwise

    Notes
    -----
    The API endpoint used is GET /api/v2/subject/{subject_id}

    If the API call fails, returns None and logs a warning.
    """
    api_host = api_host if api_host else "http://aind-metadata-service"
    subject_id = get_subject_id(nwbfile, session_info)

    configuration = aind_metadata_service_client.Configuration(host=api_host)

    with aind_metadata_service_client.ApiClient(configuration) as api_client:
        api_instance = aind_metadata_service_client.DefaultApi(api_client)

        # there are known validation issues with old subject data, try to get the content here but accept the raw response if needed
        try:
            subject_response = api_instance.get_subject(subject_id=subject_id)
            return subject_response
        except ApiException as e:
            # If validation fails, try to get the raw response
            print(f"Warning: API request failed for subject {subject_id}: attempting to parse raw response")

            # Get raw response without preload content validation
            response = api_instance.get_subject_without_preload_content(subject_id=subject_id)
            raw_data = json.loads(response.data.decode('utf-8'))

            # Try to create Subject object from raw data, fixing known issues
            # Fix null maternal_genotype issue
            if raw_data.get('subject_details', {}).get('breeding_info', {}).get('maternal_genotype') is None:
                raw_data['subject_details']['breeding_info']['maternal_genotype'] = ""
            if raw_data.get('subject_details', {}).get('breeding_info', {}).get('paternal_genotype') is None:
                raw_data['subject_details']['breeding_info']['paternal_genotype'] = ""

            # Create Subject from the fixed data
            subject = Subject(**raw_data)
            
            # Compare to nwbfile if available
            if nwbfile is not None:
                subject_sex_dict = {"F": "Female", "M": "Male"}

                assert nwbfile.subject.species == raw_data['subject_details']['species']['name'], \
                    f"Species mismatch between NWB file ({nwbfile.subject.species}) and metadata service ({raw_data['subject_details']['species']})"

                assert subject_sex_dict.get(nwbfile.subject.sex) == raw_data['subject_details']['sex'], \
                    f"Sex mismatch between NWB file ({nwbfile.subject.sex}) and metadata service ({raw_data['subject_details']['sex']})"
                
                assert get_subject_date_of_birth(nwbfile).strftime("%Y-%m-%d") == raw_data['subject_details']['date_of_birth'], \
                    f"Date of birth mismatch between NWB file ({get_subject_date_of_birth(nwbfile)}) and metadata service ({raw_data['subject_details']['date_of_birth']})" 
                
                assert nwbfile.subject.genotype == raw_data['subject_details']['genotype'], \
                    f"Genotype mismatch between NWB file ({nwbfile.subject.genotype}) and metadata service ({raw_data['subject_details']['genotype']})"

            return subject

