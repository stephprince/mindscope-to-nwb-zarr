"""Generates subject metadata from NWB files for visual behavior ephys (neuropixels) sessions.

Mirrors ``visual_behavior_ophys/subject.py`` (on-disk caching, raw-parse fallback for the
known LIMS schema quirks, JSON-safe dumping) but is deliberately *stricter*: this is a
one-time conversion of a fixed, existing dataset, so any unexpected outcome should fail
loudly rather than warn-and-continue. Concretely, where the ophys module warns and keeps
the LIMS value on a sex / date-of-birth / genotype disagreement, this module raises. An
unreachable metadata service also raises here (the ophys module returns None); we never
want a session to silently produce metadata missing its subject record.

The dataset was checked before adopting the strict policy: for every sampled VBN subject
the LIMS sex/genotype match the dataset exactly (e.g. "wt/wt", the full Cre;Ai32 allelic
form), so a mismatch genuinely signals a problem to investigate, not routine notation
drift.
"""

import json
import warnings
import pandas as pd
from datetime import datetime
from pynwb import NWBFile
from typing import Optional

from aind_data_schema.core.subject import Subject

from mindscope_to_nwb_zarr.aind_data_schema.utils import get_subject_id, get_subject_date_of_birth
from mindscope_to_nwb_zarr.aind_data_schema.metadata_service_cache import (
    load_cached, store_cached, SUBJECT,
)

import aind_metadata_service_client
from aind_metadata_service_client.rest import ApiException
from urllib3.exceptions import HTTPError as Urllib3HTTPError


def _fetch_subject_raw(subject_id: str, api_host: str) -> Optional[dict]:
    """Fetch the raw subject response dict from the AIND metadata service.

    Returns the JSON-safe raw_data dict that ``Subject(**raw_data)`` is built from
    (after the raw-parse/genotype fixups), or None if the service is unreachable.
    Raises RuntimeError if the subject is not found (HTTP 404). Does not touch the
    cache; callers cache the result.

    The None-on-unreachable contract is kept (rather than raising) so the preload
    script can distinguish "service down, retry later" from a genuine 404; the public
    ``fetch_subject_from_aind_metadata_service`` escalates the None to a hard failure.
    """
    configuration = aind_metadata_service_client.Configuration(host=api_host)

    with aind_metadata_service_client.ApiClient(configuration) as api_client:
        api_instance = aind_metadata_service_client.DefaultApi(api_client)

        try:
            subject_response = api_instance.get_subject(subject_id=subject_id)
            # Dump in JSON mode so this clean path yields the same JSON-native types
            # (dates/enums as strings) as the raw-parse fallback below. Otherwise the
            # string-based cross-checks (date_of_birth strptime, sex, genotype) would
            # crash or spuriously mismatch on native date/enum objects.
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
            # A 5xx is a service-side outage (the body is a plain error string like
            # "Internal Server Error", not JSON), distinct from the validation errors below
            # (real JSON that merely fails schema validation). Surface it clearly instead of
            # letting the raw-parse json.loads fail with a confusing JSONDecodeError.
            if e.status is not None and e.status >= 500:
                body = e.body.decode("utf-8", "replace") if isinstance(e.body, bytes) else str(e.body)
                raise RuntimeError(
                    f"AIND metadata service returned HTTP {e.status} for subject {subject_id}: {body[:200]!r}"
                ) from e
            warnings.warn(f"Validation error for subject {subject_id}, attempting to parse raw response")

            response = api_instance.get_subject_without_preload_content(subject_id=subject_id)
            try:
                raw_data = json.loads(response.data.decode('utf-8'))
            except (ValueError, UnicodeDecodeError) as pe:
                raise RuntimeError(
                    f"AIND metadata service returned a non-JSON body (HTTP {response.status}) for "
                    f"subject {subject_id}: {response.data[:200]!r}"
                ) from pe

            # Fix null maternal/paternal genotype within breeding_info, when present.
            # breeding_info itself is nullable in the schema and is null for some
            # subjects, so guard against that before calling .get on it.
            breeding_info = raw_data.get('subject_details', {}).get('breeding_info')
            if breeding_info is not None:
                if breeding_info.get('maternal_genotype') is None:
                    breeding_info['maternal_genotype'] = ""
                    warnings.warn(f"Fixed null maternal genotype for subject {subject_id}")
                if breeding_info.get('paternal_genotype') is None:
                    breeding_info['paternal_genotype'] = ""
                    warnings.warn(f"Fixed null paternal genotype for subject {subject_id}")
            return raw_data


def cross_check_subject_against_session_info(nwbfile: NWBFile, session_info: pd.Series) -> None:
    """Fail loudly if the NWB subject disagrees with the session table row.

    The session tables (behavior_sessions.csv / ecephys_sessions.csv) carry their own
    ``mouse_id``, ``sex`` and ``genotype`` per session. The mouse id is already
    cross-checked in ``get_subject_id``; this additionally checks sex and genotype. For
    this fixed dataset every field must agree, so a mismatch raises.

    Raises
    ------
    ValueError
        If the session table's sex or genotype disagrees with the NWB subject.
    """
    # Mouse id: asserts session_info['mouse_id'] == nwbfile.subject.subject_id.
    get_subject_id(nwbfile, session_info)

    if str(session_info['sex']) != str(nwbfile.subject.sex):
        raise ValueError(
            f"Sex mismatch between session table ({session_info['sex']}) and NWB "
            f"({nwbfile.subject.sex}) for mouse {nwbfile.subject.subject_id}."
        )
    if str(session_info['genotype']) != str(nwbfile.subject.genotype):
        raise ValueError(
            f"Genotype mismatch between session table ({session_info['genotype']}) and NWB "
            f"({nwbfile.subject.genotype}) for mouse {nwbfile.subject.subject_id}."
        )


def fetch_subject_from_aind_metadata_service(
    nwbfile: NWBFile,
    session_info: pd.Series,
    api_host: Optional[str] = None,
) -> Subject:
    """
    Fetch subject metadata from the AIND metadata service API (fail-loud).

    Parameters
    ----------
    nwbfile : NWBFile
        The NWB file containing subject information for validation
    session_info : pd.Series
        Series containing session information (from the behavior or ecephys session table)
    api_host : str, optional
        The API host URL. Defaults to "http://aind-metadata-service"

    Returns
    -------
    Subject
        Subject object built from the (authoritative) metadata-service response.

    Raises
    ------
    RuntimeError
        If the metadata service is unreachable, if the subject is not found (HTTP 404),
        or if the LIMS genotype is missing from both LIMS and the NWB.
    AssertionError
        If the LIMS response disagrees with the NWB file on species.
    ValueError
        If the LIMS response disagrees with the NWB file on sex, date of birth (beyond a
        small integer-day-age tolerance), or genotype.

    Notes
    -----
    The API endpoint used is GET /api/v2/subject/{subject_id}

    The subject_id is the 6-digit mouse ID from the NWB file, cross-checked against
    session_info['mouse_id'] (see ``get_subject_id``).

    The metadata service (LIMS) is treated as authoritative: the returned Subject is built
    from its response. Unlike the ophys pipeline, disagreements with the NWB are fatal
    here (see module docstring): this is a one-time conversion of a fixed dataset, so an
    unexpected mismatch should surface and be addressed, not be silently accepted.
    """
    api_host = api_host if api_host else "http://aind-metadata-service"

    # Cross-check the NWB subject against the session table row (mouse id, sex, genotype)
    # before hitting the metadata service, so a table/NWB disagreement fails loudly.
    cross_check_subject_against_session_info(nwbfile, session_info)

    # 6-digit mouse ID from the NWB file, cross-checked against session_info['mouse_id'].
    subject_id = get_subject_id(nwbfile, session_info)

    # The metadata-service response for a subject does not change between runs and the
    # same subject recurs across many sessions, so cache it by subject_id (see
    # metadata_service_cache). On a miss, fetch and cache. An unreachable service yields
    # None from the fetcher, which we escalate to a hard failure below (fail-loud).
    raw_data = load_cached(SUBJECT, subject_id)
    if raw_data is None:
        raw_data = _fetch_subject_raw(subject_id, api_host)
        if raw_data is None:
            raise RuntimeError(
                f"AIND metadata service unreachable while fetching subject {subject_id}."
            )
        store_cached(SUBJECT, subject_id, raw_data)

    # Cross-check the AIND metadata service (LIMS) response against the NWB file. The LIMS
    # record is authoritative and is what the Subject is built from below, but every field
    # is expected to agree for this dataset, so any disagreement fails loudly.
    subject_sex_dict = {"F": "Female", "M": "Male"}

    assert nwbfile.subject.species == raw_data['subject_details']['species']['name'], \
        f"Species mismatch: NWB={nwbfile.subject.species}, API={raw_data['subject_details']['species']['name']}"

    if subject_sex_dict.get(nwbfile.subject.sex) != raw_data['subject_details']['sex']:
        raise ValueError(
            f"Sex mismatch for subject {subject_id}: NWB={nwbfile.subject.sex}, "
            f"LIMS={raw_data['subject_details']['sex']}."
        )

    # The NWB stores only an integer-day age (P<days>D), so the DOB derived from it
    # (session_start_time - age) is approximate to within ~1 day. Allow a small tolerance
    # against the LIMS date_of_birth; anything larger is a real discrepancy and fails.
    nwb_dob = get_subject_date_of_birth(nwbfile)
    api_dob = datetime.strptime(raw_data['subject_details']['date_of_birth'], "%Y-%m-%d").date()
    if abs((nwb_dob - api_dob).days) > 2:
        raise ValueError(
            f"Date of birth mismatch >2 days for subject {subject_id}: NWB={nwb_dob}, "
            f"LIMS={api_dob}."
        )

    # Genotype. LIMS is authoritative when it has a value. LIMS occasionally leaves
    # genotype null while the NWB records it; in that case backfill from the NWB (the
    # value is not missing, only absent from LIMS) and fail only if it is missing from
    # both. When both are present they must agree (they do for this dataset), so a true
    # mismatch fails loudly.
    lims_genotype = raw_data['subject_details']['genotype']
    nwb_genotype = nwbfile.subject.genotype
    if lims_genotype is None:
        if not nwb_genotype:
            raise RuntimeError(
                f"Genotype for subject {subject_id} is missing from both the AIND metadata "
                f"service and the NWB file."
            )
        raw_data['subject_details']['genotype'] = nwb_genotype
        warnings.warn(
            f"Genotype is null in LIMS for subject {subject_id}; backfilled from the NWB "
            f"file: {nwb_genotype!r}."
        )
    elif nwb_genotype != lims_genotype:
        raise ValueError(
            f"Genotype mismatch for subject {subject_id}: NWB={nwb_genotype}, "
            f"LIMS={lims_genotype}."
        )

    # Build the aind_data_schema Subject from the response dict -- the same for both the
    # clean-success and raw-parse fallback paths, so the return type is consistent.
    return Subject(**raw_data)
