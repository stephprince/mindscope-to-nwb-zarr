"""Generates subject metadata from NWB files for visual coding ephys sessions"""

import json
import warnings
import pandas as pd
from datetime import datetime
from pynwb import NWBFile
from typing import Optional

from aind_data_schema.core.subject import Subject

from mindscope_to_nwb_zarr.aind_data_schema.utils import get_subject_date_of_birth
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.instrument import (
    get_acquisition_start_time,
    get_experiment_metadata,
    get_mouse_id,
    _load_subject_mapping,
)

import aind_metadata_service_client
from aind_metadata_service_client.rest import ApiException
from urllib3.exceptions import HTTPError as Urllib3HTTPError


def cross_check_mouse_id(nwbfile: NWBFile, session_info: pd.Series, subject_mapping_path: str) -> None:
    """Fail loudly if the 6-digit mouse id is inconsistent across the reference sources.

    The experiment metadata CSV maps ``session_id`` (the sessions.csv row driving
    generation) directly to a 6-digit mouse id. The subject mapping JSON maps the NWB
    ``subject_id`` to the same 6-digit mouse id. Both must agree: the mouse id keys the
    metadata-service subject/procedures lookups, so a disagreement would mean fetching
    the wrong animal's records. A mismatch, or an NWB subject id missing from the
    mapping, therefore raises.

    Note: sessions.csv's ``specimen_id`` is a different id space than the subject
    mapping keys (the NWB subject ids), so it cannot be joined to the mapping directly.
    The one session absent from the experiment CSV (819701982) cannot be cross-checked
    and is skipped with a warning (expected, documented).

    Raises
    ------
    KeyError
        If the NWB subject id is not present in the subject mapping.
    ValueError
        If the experiment CSV and the subject mapping disagree on the mouse id.
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
        raise KeyError(
            f"Session {session_id}: NWB subject id {nwb_subject_id} not found in the subject "
            f"mapping; cannot resolve the 6-digit mouse id (experiment CSV says {expected_mouse_id})."
        )
    if str(mapped_mouse_id) != expected_mouse_id:
        raise ValueError(
            f"Session {session_id}: mouse id mismatch - experiment CSV says {expected_mouse_id}, "
            f"but subject mapping (via NWB subject id {nwb_subject_id}) says {mapped_mouse_id}."
        )


def fetch_subject_from_aind_metadata_service(
    nwbfile: NWBFile,
    session_info: pd.Series,
    api_host: Optional[str] = None,
    subject_mapping_path: Optional[str] = None,
) -> Optional[Subject]:
    """
    Fetch subject metadata from AIND metadata service API

    Parameters
    ----------
    nwbfile : NWBFile
        The NWB file containing subject information for validation
    session_info : pd.Series
        Series containing session information (from sessions.csv)
    api_host : str, optional
        The API host URL. Defaults to "http://aind-metadata-service"
    subject_mapping_path : str, optional
        Path to the NWB subject_id -> 6-digit mouse id mapping JSON. Defaults to the
        bundled copy resolved by ``get_mouse_id``.

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

    The subject_id from the NWB files is a 9-digit LIMS id, not the 6-digit mouse id
    the metadata service expects; it is resolved via the subject mapping.

    The metadata service (LIMS) is treated as authoritative: the returned Subject is
    built from its response, and on a sex, date-of-birth, or genotype disagreement with
    the NWB the LIMS value is kept and a warning is emitted.

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
            subject_response = api_instance.get_subject(subject_id=subject_id)
            # Dump in JSON mode so this clean path yields the same JSON-native types
            # (dates/enums as strings) as the raw-parse fallback below. Otherwise the
            # string-based cross-checks (date_of_birth strptime, sex, genotype) would
            # crash or spuriously mismatch on native date/enum objects.
            raw_data = subject_response.model_dump(mode='json') if hasattr(subject_response, 'model_dump') else subject_response
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

            # Fix null maternal/paternal genotype within breeding_info, when present.
            # breeding_info itself is nullable in the schema and is null for many subjects
            # (see the top-level genotype backfill below); guard against that so we do not
            # call .get on None.
            breeding_info = raw_data.get('subject_details', {}).get('breeding_info')
            if breeding_info is not None:
                if breeding_info.get('maternal_genotype') is None:
                    breeding_info['maternal_genotype'] = ""
                    warnings.warn(f"Fixed null maternal genotype for subject {subject_id}")
                if breeding_info.get('paternal_genotype') is None:
                    breeding_info['paternal_genotype'] = ""
                    warnings.warn(f"Fixed null paternal genotype for subject {subject_id}")

        # Cross-check the AIND metadata service (LIMS) response against the NWB file.
        # The LIMS record is authoritative and is what the Subject is built from below.
        # Species must agree (a mismatch raises loudly). Sex, date of birth, and genotype
        # only warn: the NWB files and LIMS are known to disagree for some subjects, so we
        # keep the LIMS value and surface the discrepancy.
        subject_sex_dict = {"F": "Female", "M": "Male"}

        assert nwbfile.subject.species == raw_data['subject_details']['species']['name'], \
            f"Species mismatch: NWB={nwbfile.subject.species}, API={raw_data['subject_details']['species']['name']}"

        if subject_sex_dict.get(nwbfile.subject.sex) != raw_data['subject_details']['sex']:
            warnings.warn(
                f"Sex mismatch for subject {subject_id}: NWB={nwbfile.subject.sex}, "
                f"LIMS={raw_data['subject_details']['sex']}. Using the LIMS value."
            )

        # The NWB stores only an integer-day age (P<days>D), so the DOB derived from it
        # (acquisition_start - age) is approximate. The NWB session_start_time is a
        # packaging date, so anchor to the corrected acquisition start from the reference
        # CSV. Compare against the LIMS date_of_birth with a tolerance and warn (not fail).
        acquisition_start_time = get_acquisition_start_time(nwbfile, session_info)
        nwb_dob = get_subject_date_of_birth(nwbfile, acquisition_start_time)
        api_dob = datetime.strptime(raw_data['subject_details']['date_of_birth'], "%Y-%m-%d").date()
        if abs((nwb_dob - api_dob).days) > 2:
            warnings.warn(
                f"Date of birth mismatch >2 days for subject {subject_id}: NWB={nwb_dob}, "
                f"LIMS={api_dob}. Using the LIMS value."
            )

        # Genotype. LIMS is authoritative when it has a value, but it leaves genotype null
        # for ~half the Neuropixels subjects (wildtype mice) while the NWB records it (e.g.
        # "wt/wt"). When LIMS is null, backfill from the NWB -- the value is not actually
        # missing, only absent from LIMS -- and fail loudly only if it is missing from both
        # sources. When both are present but differ (known notation differences), keep the
        # LIMS value and warn. See the README (Visual Coding Neuropixels genotype handling).
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
            warnings.warn(
                f"Genotype mismatch for subject {subject_id}: NWB={nwb_genotype}, "
                f"LIMS={lims_genotype}. Using the LIMS value."
            )

        # Build the aind_data_schema Subject from the response dict -- the same for both
        # the clean-success and raw-parse fallback paths, so the return type is consistent.
        return Subject(**raw_data)
