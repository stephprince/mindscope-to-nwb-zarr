"""Generates procedures metadata from NWB files for visual behavior ophys sessions"""

import json
import warnings
import pandas as pd
from pynwb import NWBFile
from typing import Optional

from aind_data_schema.core.procedures import Procedures

from mindscope_to_nwb_zarr.aind_data_schema.utils import get_subject_id
from mindscope_to_nwb_zarr.aind_data_schema.metadata_service_cache import (
    load_cached, store_cached, PROCEDURES,
)

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


def _fetch_procedures_raw(subject_id: str, api_host: str) -> Optional[dict]:
    """Fetch the raw procedures response dict from the AIND metadata service.

    Returns the JSON-safe raw_data dict that ``Procedures(**raw_data)`` is built from
    (after the known-validation-issue fixups), or None if the service is unreachable.
    Raises RuntimeError if the procedures are not found (HTTP 404). Does not touch the
    cache; callers cache the result.
    """
    configuration = aind_metadata_service_client.Configuration(host=api_host)

    with aind_metadata_service_client.ApiClient(configuration) as api_client:
        api_instance = aind_metadata_service_client.DefaultApi(api_client)

        try:
            procedures_response = api_instance.get_procedures(subject_id=subject_id)
            if hasattr(procedures_response, 'model_dump'):
                return procedures_response.model_dump(mode="json")
            return procedures_response
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
            return raw_data


def fetch_procedures_from_aind_metadata_service(
    nwbfile: NWBFile,
    session_info: pd.Series,
    api_host: Optional[str] = None,
) -> Optional[Procedures]:
    """
    Fetch procedures metadata from AIND metadata service API

    Parameters
    ----------
    nwbfile : NWBFile
        The NWB file used to resolve (and cross-check) the subject ID
    session_info : pd.Series
        Series containing session information (from the behavior session or ophys
        experiment table)
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

    The subject_id is the 6-digit mouse ID from the NWB file, cross-checked against
    session_info['mouse_id'] (see ``get_subject_id``).

    If the metadata service cannot be reached, returns None and logs a warning.
    """
    api_host = api_host if api_host else "http://aind-metadata-service"

    # 6-digit mouse ID from the NWB file, cross-checked against session_info['mouse_id'].
    subject_id = get_subject_id(nwbfile, session_info)

    # The procedures record for a subject does not change between runs and the same
    # subject recurs across many sessions, so cache it by subject_id (see
    # metadata_service_cache). On a miss, fetch and cache; on an unreachable service the
    # fetcher returns None and we propagate that (no caching of a failure).
    raw_data = load_cached(PROCEDURES, subject_id)
    if raw_data is None:
        raw_data = _fetch_procedures_raw(subject_id, api_host)
        if raw_data is None:
            return None
        store_cached(PROCEDURES, subject_id, raw_data)

    # Build the aind_data_schema Procedures from the response dict -- the same for both
    # the clean-success and raw-parse fallback paths, so the return type is consistent.
    return Procedures(**raw_data)
