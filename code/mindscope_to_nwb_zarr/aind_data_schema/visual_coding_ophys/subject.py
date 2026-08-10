"""Generates subject metadata from NWB files for visual coding ophys sessions"""

import json
import warnings
import pandas as pd
from datetime import datetime
from pynwb import NWBFile
from typing import Optional

from aind_data_schema.core.subject import Subject

from mindscope_to_nwb_zarr.aind_data_schema.utils import get_subject_date_of_birth
from mindscope_to_nwb_zarr.aind_data_schema.metadata_service_cache import (
    load_cached, store_cached, SUBJECT,
)

import aind_metadata_service_client
from aind_metadata_service_client.rest import ApiException
from urllib3.exceptions import HTTPError as Urllib3HTTPError


def _fetch_subject_raw(subject_id: str, api_host: str) -> Optional[dict]:
    """Fetch the raw subject response dict from the AIND metadata service.

    Returns the JSON-safe raw_data dict that ``Subject(**raw_data)`` is built from
    (after the genotype fixups), or None if the service is unreachable. Raises
    RuntimeError if the subject is not found (HTTP 404). Does not touch the cache;
    callers cache the result.
    """
    configuration = aind_metadata_service_client.Configuration(host=api_host)

    with aind_metadata_service_client.ApiClient(configuration) as api_client:
        api_instance = aind_metadata_service_client.DefaultApi(api_client)

        try:
            subject_response = api_instance.get_subject(subject_id=subject_id)
            if hasattr(subject_response, 'model_dump'):
                return subject_response.model_dump(mode='json')
            return subject_response
        except Urllib3HTTPError as e:
            warnings.warn(f"Could not connect to AIND metadata service at {api_host}: {e}")
            return None
        except ApiException as e:
            if e.status == 404:
                raise RuntimeError(
                    f"Subject {subject_id} not found in the AIND metadata service (HTTP 404)."
                ) from e
            warnings.warn(f"Validation error for subject {subject_id}, attempting to parse raw response")

            response = api_instance.get_subject_without_preload_content(subject_id=subject_id)
            raw_data = json.loads(response.data.decode('utf-8'))

            # Fix null genotype issues
            if raw_data.get('subject_details', {}).get('breeding_info', {}).get('maternal_genotype') is None:
                raw_data['subject_details']['breeding_info']['maternal_genotype'] = ""
                warnings.warn(f"Fixed null maternal genotype for subject {subject_id}")
            if raw_data.get('subject_details', {}).get('breeding_info', {}).get('paternal_genotype') is None:
                raw_data['subject_details']['breeding_info']['paternal_genotype'] = ""
                warnings.warn(f"Fixed null paternal genotype for subject {subject_id}")
            return raw_data


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
        Subject object if found; None if the metadata service is unreachable.

    Raises
    ------
    RuntimeError
        If the subject is not found in the metadata service (HTTP 404). This fails the
        whole session loudly so the missing subject is surfaced and addressed.
    AssertionError
        If the LIMS (metadata service) response does not match the NWB file for
        species. Species must never differ, so a mismatch fails the whole session
        loudly. Sex, date-of-birth, and genotype mismatches only warn (see Notes)
        because the NWB files and LIMS are known to disagree for some subjects.

    Notes
    -----
    The API endpoint used is GET /api/v2/subject/{subject_id}

    The subject_id from the NWB v2 files on DANDI is not used.
    The subject_id is a 6-digit mouse ID extracted from session_info['specimen']['donor']['external_donor_name'].

    The metadata service (LIMS) is treated as authoritative: the returned Subject is
    built from its response, and on a sex, date-of-birth, or genotype disagreement with
    the NWB the LIMS value is kept and a warning is emitted.

    If the metadata service cannot be reached, returns None and logs a warning.
    """
    api_host = api_host if api_host else "http://aind-metadata-service"

    # Get subject ID from session metadata (external_donor_name is the 6-digit mouse ID)
    subject_id = session_info['specimen']['donor']['external_donor_name']

    # Cache the metadata-service response by subject_id: it does not change between runs
    # and the same subject recurs across sessions (see metadata_service_cache). On a miss,
    # fetch and cache; on an unreachable service the fetcher returns None (nothing cached).
    raw_data = load_cached(SUBJECT, subject_id)
    if raw_data is None:
        raw_data = _fetch_subject_raw(subject_id, api_host)
        if raw_data is None:
            return None
        store_cached(SUBJECT, subject_id, raw_data)

    # Cross-check the AIND metadata service (LIMS) response against the NWB file.
    # The LIMS record is authoritative and is what the Subject is built from below.
    # Species must agree (a mismatch raises loudly). Sex, date of birth, and genotype
    # only warn: the NWB files and LIMS are known to disagree for some subjects (see
    # the README), so we keep the LIMS value and surface the discrepancy.
    subject_sex_dict = {"F": "Female", "M": "Male"}

    assert nwbfile.subject.species == raw_data['subject_details']['species']['name'], \
        f"Species mismatch: NWB={nwbfile.subject.species}, API={raw_data['subject_details']['species']['name']}"

    if subject_sex_dict.get(nwbfile.subject.sex) != raw_data['subject_details']['sex']:
        warnings.warn(
            f"Sex mismatch for subject {subject_id}: NWB={nwbfile.subject.sex}, "
            f"LIMS={raw_data['subject_details']['sex']}. Using the LIMS value."
        )

    # The NWB stores only an integer-day age (P<days>D), so the DOB derived from
    # it (acquisition_date - age) is approximate. Compare against the LIMS
    # date_of_birth with a small tolerance and warn (rather than fail) on mismatch.
    nwb_dob = get_subject_date_of_birth(nwbfile)
    api_dob = datetime.strptime(raw_data['subject_details']['date_of_birth'], "%Y-%m-%d").date()
    if abs((nwb_dob - api_dob).days) > 2:
        warnings.warn(
            f"Date of birth mismatch >2 days for subject {subject_id}: NWB={nwb_dob}, "
            f"LIMS={api_dob}. Using the LIMS value."
        )

    # The NWB and LIMS sometimes record the same genotype in different notations
    # (e.g. a short form vs the full allelic form), so warn rather than fail.
    if nwbfile.subject.genotype != raw_data['subject_details']['genotype']:
        warnings.warn(
            f"Genotype mismatch for subject {subject_id}: NWB={nwbfile.subject.genotype}, "
            f"LIMS={raw_data['subject_details']['genotype']}. Using the LIMS value."
        )

    # Build the aind_data_schema Subject from the response dict -- the same for both
    # the clean-success and raw-parse fallback paths, so the return type is consistent.
    return Subject(**raw_data)
