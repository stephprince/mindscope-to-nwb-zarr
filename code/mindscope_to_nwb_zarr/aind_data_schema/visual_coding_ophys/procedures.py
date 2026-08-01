"""Generates procedures metadata from NWB files for visual coding ophys sessions"""

import json
import warnings
import pandas as pd
from typing import Optional

from aind_data_schema.core.procedures import Procedures

import aind_metadata_service_client
from aind_metadata_service_client.rest import ApiException
from urllib3.exceptions import HTTPError as Urllib3HTTPError


def _fix_procedures_validation_issues(subject_procedures: list) -> None:
    """
    Fix known validation issues in procedures data from the API, in place.

    Parameters
    ----------
    subject_procedures : list
        The subject_procedures list from the API response; modified in place.
    """
    for i, procedure in enumerate(subject_procedures):
        # Fix Surgery procedures
        if procedure.get('object_type') == 'Surgery':
            if procedure.get('anaesthesia') is not None and 'duration' not in procedure['anaesthesia']:
                subject_procedures[i]['anaesthesia']['duration'] = 0.0
                warnings.warn("Fixed missing anaesthesia.duration, set to 0.0")

            # Fix procedures within Surgery
            for j, surgery_proc in enumerate(procedure['procedures']):
                # Fix Craniotomy position (should be list or Translation object, not string)
                if surgery_proc.get('object_type') == 'Craniotomy' and 'position' in surgery_proc:
                    position = surgery_proc['position']
                    if isinstance(position, str):
                        subject_procedures[i]['procedures'][j]['position'] = [position]
                        warnings.warn(f"Fixed Craniotomy position from string '{position}' to list [{position}]")


def fetch_procedures_from_aind_metadata_service(
    session_info: pd.Series,
    api_host: Optional[str] = None,
) -> Optional[Procedures]:
    """
    Fetch procedures metadata from AIND metadata service API

    Parameters
    ----------
    session_info : pd.Series
        Series containing session information (from ophys_experiments.json)
    api_host : str, optional
        The API host URL. Defaults to "http://aind-metadata-service"

    Returns
    -------
    Procedures or None
        Procedures object if found, None if the metadata service is unreachable.

    Raises
    ------
    RuntimeError
        If the procedures for the subject are not found in the metadata service
        (HTTP 404). This fails the whole session loudly so the missing record is
        surfaced and addressed.

    Notes
    -----
    The API endpoint used is GET /api/v2/procedures/{subject_id}

    The subject_id from the NWB v2 files on DANDI is not used.
    The subject_id is a 6-digit mouse ID extracted from session_info['specimen']['donor']['external_donor_name'].

    If the metadata service cannot be reached, returns None and logs a warning.
    """
    api_host = api_host if api_host else "http://aind-metadata-service"

    # Get subject ID from session metadata (external_donor_name is the 6-digit mouse ID)
    subject_id = session_info['specimen']['donor']['external_donor_name']

    configuration = aind_metadata_service_client.Configuration(host=api_host)

    with aind_metadata_service_client.ApiClient(configuration) as api_client:
        api_instance = aind_metadata_service_client.DefaultApi(api_client)

        try:
            procedures_response = api_instance.get_procedures(subject_id=subject_id)
            raw_data = procedures_response.model_dump() if hasattr(procedures_response, 'model_dump') else procedures_response
        except Urllib3HTTPError as e:
            warnings.warn(f"Could not connect to AIND metadata service at {api_host}: {e}")
            return None
        except ApiException as e:
            if e.status == 404:
                raise RuntimeError(
                    f"Procedures for subject {subject_id} not found in the AIND metadata service (HTTP 404)."
                ) from e
            warnings.warn(f"Validation error for procedures (subject {subject_id}), attempting to parse and fix raw response")

            response = api_instance.get_procedures_without_preload_content(subject_id=subject_id)
            raw_data = json.loads(response.data.decode('utf-8'))

            # Fix known validation issues in procedures data (in place)
            _fix_procedures_validation_issues(raw_data['subject_procedures'])

        # Build the aind_data_schema Procedures from the response dict -- the same for both
        # the clean-success and raw-parse fallback paths, so the return type is consistent.
        return Procedures(**raw_data)
