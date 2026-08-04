"""Generates procedures metadata from NWB files for visual coding ephys sessions"""

import json
import warnings
from pathlib import Path
from pynwb import NWBFile
from typing import Optional

from aind_data_schema.core.procedures import Procedures

from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.instrument import get_mouse_id

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
    nwbfile: NWBFile,
    api_host: Optional[str] = None,
    subject_mapping_path: Optional[Path] = None,
) -> Optional[Procedures]:
    """
    Fetch procedures metadata from AIND metadata service API

    Parameters
    ----------
    nwbfile : NWBFile
        The NWB file whose subject is looked up. The Visual Coding Neuropixels NWB
        files store a 9-digit LIMS id as ``subject.subject_id``; it is mapped to the
        6-digit mouse id the metadata service is keyed by (see ``get_mouse_id``).
    api_host : str, optional
        The API host URL. Defaults to "http://aind-metadata-service"
    subject_mapping_path : Path, optional
        Path to the NWB subject_id -> 6-digit mouse id mapping JSON. Defaults to the
        bundled copy resolved by ``get_mouse_id``.

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

    The subject_id from the NWB files is a 9-digit LIMS id, not the 6-digit mouse id
    the metadata service expects; it is resolved via the subject mapping.

    If the metadata service cannot be reached, returns None and logs a warning.
    """
    api_host = api_host if api_host else "http://aind-metadata-service"

    # The NWB subject_id is a 9-digit LIMS id; resolve the 6-digit mouse id the
    # metadata service is keyed by.
    subject_id = get_mouse_id(nwbfile, subject_mapping_path)

    configuration = aind_metadata_service_client.Configuration(host=api_host)

    with aind_metadata_service_client.ApiClient(configuration) as api_client:
        api_instance = aind_metadata_service_client.DefaultApi(api_client)

        try:
            procedures_response = api_instance.get_procedures(subject_id=subject_id)
            # Dump in JSON mode so this clean path yields the same JSON-native types as
            # the raw-parse fallback below, keeping Procedures(**raw_data) identical.
            raw_data = procedures_response.model_dump(mode='json') if hasattr(procedures_response, 'model_dump') else procedures_response
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
