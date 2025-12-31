"""Generates an example JSON file for visual behavior ephys procedures"""

import json
import pandas as pd
import warnings
from pynwb import NWBFile
from typing import Optional

from aind_data_schema.core.procedures import Procedures

from mindscope_to_nwb_zarr.aind_data_schema.utils import get_subject_id

import aind_metadata_service_client
from aind_metadata_service_client.rest import ApiException
from urllib3.exceptions import HTTPError as Urllib3HTTPError

def _fix_procedures_validation_issues(subject_procedures: list) -> list:
    """
    Fix known validation issues in procedures data from the API

    Parameters
    ----------
    subject_procedures : list
        The subject_procedures list from the API response

    Returns
    -------
    list
        The fixed subject_procedures list
    """
    # Fix issues in subject_procedures
    for i, procedure in enumerate(subject_procedures):
        # Fix Surgery procedures
        if procedure.get('object_type') == 'Surgery':
            if procedure.get('anaesthesia') is not None and 'duration' not in procedure['anaesthesia']:
                subject_procedures[i]['anaesthesia']['duration'] = 0.0 # TODO - is there a better missing value?
                print(f"  Fixed missing anaesthesia.duration, set to 0.0")

            # Fix procedures within Surgery
            for j, surgery_proc in enumerate(procedure['procedures']):
                # Fix Craniotomy position (should be list or Translation object, not string)
                if surgery_proc.get('object_type') == 'Craniotomy' and 'position' in surgery_proc:
                    position = surgery_proc['position']
                    if isinstance(position, str):
                        subject_procedures[i]['procedures'][j]['position'] = [position]
                        print(f"  Fixed Craniotomy position from string '{position}' to list [{position}]")

    return subject_procedures


def fetch_procedures_from_aind_metadata_service(nwbfile: NWBFile, session_info: pd.DataFrame, api_host: Optional[str] = None) -> Optional[Procedures]:
    """
    Fetch procedures metadata from AIND metadata service API

    Parameters
    ----------
    nwbfile : NWBFile
        The NWB file containing subject information to extract subject ID
    session_info : pd.DataFrame
        DataFrame containing session information to extract subject ID
    api_host : str, optional
        The API host URL. Defaults to "http://aind-metadata-service"

    Returns
    -------
    Procedures or None
        Procedures object if found, None otherwise

    Notes
    -----
    The API endpoint used is GET /api/v2/procedures/{subject_id}

    If the API call fails, returns None and logs a warning.
    """
    api_host = api_host if api_host else "http://aind-metadata-service"
    subject_id = get_subject_id(nwbfile, session_info)

    configuration = aind_metadata_service_client.Configuration(host=api_host)

    with aind_metadata_service_client.ApiClient(configuration) as api_client:
        api_instance = aind_metadata_service_client.DefaultApi(api_client)

        # there are known validation issues with old procedures data, always get raw response
        # TODO - what will happen if no response, keep track of sessions with missing info
        try:
            procedures_response = api_instance.get_procedures(subject_id=subject_id)
            procedures = Procedures(**procedures_response)
        except Urllib3HTTPError as e:
            warnings.warn(f"Warning: Could not connect to AIND metadata service at {api_host}: {e}")
            return None
        except ApiException as e:
            # If validation fails, try to get the raw response
            warnings.warn(f"Warning: Validation error for procedures (subject {subject_id}), attempting to parse and fix raw response")

            # Get raw response without preload content validation
            response = api_instance.get_procedures_without_preload_content(subject_id=subject_id)
            raw_data = json.loads(response.data.decode('utf-8'))

            # Fix known validation issues in procedures data
            raw_data['subject_procedures'] = _fix_procedures_validation_issues(raw_data['subject_procedures'])

            # Create Procedures from the fixed data
            procedures = Procedures(**raw_data)
        return procedures
        
