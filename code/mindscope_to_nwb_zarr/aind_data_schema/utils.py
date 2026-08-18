
import json
import re
import shutil
import zipfile
import numpy as np
import pandas as pd
import warnings

from aind_data_schema_models.brain_atlas import CCFv3
from aind_data_schema_models.coordinates import AxisName, Direction, Origin
from aind_data_schema.components.stimulus import VisualStimulation, PulseShape
from aind_data_schema.components.configs import ProbeConfig
from aind_data_schema.components.coordinates import Axis, CoordinateSystem, CoordinateSystemLibrary, Rotation, Translation
from aind_data_schema_models.units import AngleUnit, SizeUnit, TimeUnit
from aind_data_schema_models.stimulus_modality import StimulusModality
from aind_data_schema.components.identifiers import Software, Code
from aind_data_schema.core.acquisition import StimulusEpoch
from mindscope_to_nwb_zarr.aind_data_schema.stimuli import OptotaggingStimulation

from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo
from functools import lru_cache
from pathlib import Path
from pynwb import NWBFile


@lru_cache(maxsize=None)
def _first_use_dates(table_paths: tuple) -> dict:
    """Map each rig ``equipment_name`` to its earliest ``date_of_acquisition`` (a date).

    Computed once (cached) from the given session-table CSV paths, each of which must have
    ``equipment_name`` and ``date_of_acquisition`` columns. ``date_of_acquisition`` values
    have mixed ISO-8601 formats (some with sub-second precision), so are parsed with
    ``format="ISO8601"``.
    """
    frames = [pd.read_csv(path, usecols=["equipment_name", "date_of_acquisition"]) for path in table_paths]
    rows = pd.concat(frames, ignore_index=True).dropna(subset=["equipment_name", "date_of_acquisition"])
    day = pd.to_datetime(rows["date_of_acquisition"], utc=True, format="ISO8601").dt.date
    return rows.assign(_day=day).groupby("equipment_name")["_day"].min().to_dict()


def first_use_date(equipment_name: str, table_paths) -> date:
    """Earliest ``date_of_acquisition`` for a rig across the given session tables (a date).

    Used to set an instrument's ``modification_date`` to the first day the rig was used in
    the dataset.

    Raises
    ------
    KeyError
        If the rig never appears in the tables (so its instrument cannot be dated).
    """
    dates = _first_use_dates(tuple(str(p) for p in table_paths))
    first = dates.get(equipment_name)
    if first is None:
        raise KeyError(
            f"No sessions with equipment_name {equipment_name!r} in the session tables; "
            f"cannot set the instrument modification_date."
        )
    return first


# CSV mapping subject (donor/mouse) id -> ethics (IACUC) review id, bundled in the repo.
_ETHICS_REVIEW_CSV = Path(__file__).resolve().parents[2] / "reference" / "ethics_review_ids.csv"


@lru_cache(maxsize=None)
def _load_subject_to_ethics_review(csv_path: str) -> dict:
    """Load the subject_id -> ethics_review_id mapping from the CSV (cached)."""
    df = pd.read_csv(csv_path, usecols=["subject_id", "ethics_review_id"])
    return dict(zip(df["subject_id"].astype(int), df["ethics_review_id"].astype(int)))


def get_ethics_review_id(subject_id, csv_path=None) -> list[str]:
    """Look up the ethics review id(s) for a subject.

    Parameters
    ----------
    subject_id : int | str
        The subject's donor/mouse id (e.g. ``"244896"``).
    csv_path : str | Path, optional
        Path to the subject->ethics_review_id mapping CSV. Defaults to the bundled copy.

    Returns
    -------
    list[str]
        A single-element list with the ethics review id as a string (the
        Acquisition field is a list).

    Raises
    ------
    KeyError
        If the subject is not present in the mapping CSV.
    """
    mapping = _load_subject_to_ethics_review(str(csv_path or _ETHICS_REVIEW_CSV))
    review_id = mapping.get(int(subject_id))
    if review_id is None:
        raise KeyError(
            f"No ethics_review_id found for subject_id {subject_id!r} in {_ETHICS_REVIEW_CSV}"
        )
    return [str(review_id)]


def zip_session_metadata(session_dir: Path, output_dir: Path) -> Path:
    """Bundle a session's generated metadata files into a single zip and drop the folder.

    ``write_standard_file`` writes the (up to five) metadata JSON files into
    ``session_dir`` (named for the data asset). This zips those files into
    ``output_dir/<session_dir.name>.zip`` and removes the now-redundant loose folder, so
    ``output_dir`` ends up holding only one zip per session. The zip stores the JSON files
    flat (no internal directory).

    Shared by every dataset's batch pipeline (Visual Coding ephys/ophys and Visual Behavior
    ephys), which all produce the same per-session folder of AIND metadata JSON files.

    Parameters
    ----------
    session_dir : Path
        Directory holding the session's written metadata JSON files.
    output_dir : Path
        Directory the per-session zip is written into.

    Returns
    -------
    Path
        Path to the created zip file.
    """
    zip_path = output_dir / f"{session_dir.name}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for json_file in sorted(session_dir.glob("*.json")):
            zf.write(json_file, arcname=json_file.name)
    shutil.rmtree(session_dir, ignore_errors=True)
    return zip_path


# Data asset name datetimes are timezone-free, e.g. "2018-06-27_14-07-11".
DATA_ASSET_NAME_DATETIME_FORMAT = "%Y-%m-%d_%H-%M-%S"


def build_data_asset_name(subject_id, acquisition_start_time: datetime,
                          packaging_time: datetime) -> str:
    """Build the data asset name ``<subject id>_<acquisition start>_nwb_<packaging date>``.

    Both datetimes are formatted timezone-free as ``YYYY-MM-DD_hh-mm-ss``. The packaging
    date is when the asset is produced (typically ``datetime.now()``).
    """
    fmt = DATA_ASSET_NAME_DATETIME_FORMAT
    return (
        f"{subject_id}"
        f"_{acquisition_start_time.strftime(fmt)}"
        f"_nwb_{packaging_time.strftime(fmt)}"
    )


def get_subject_id(nwbfile: NWBFile, session_info: pd.Series = None) -> str:
    """Get the subject ID from the NWB file, cross-checked with the session info. e.g., "457841".
    """
    if session_info is not None:
        assert session_info['mouse_id'] == int(nwbfile.subject.subject_id), "subject_id mismatch occurred"
    return nwbfile.subject.subject_id


def get_subject_date_of_birth(nwbfile: NWBFile, acquisition_start_time: datetime = None) -> datetime.date:
    """Calculate the animal's date of birth from age and acquisition date in NWB file.

    The age stored in the NWB file (``P<days>D``) is relative to acquisition, so the DOB
    is ``acquisition_start_time - age``. By default the anchor is
    ``nwbfile.session_start_time``; pass ``acquisition_start_time`` to re-anchor to a
    corrected start (e.g. when the file's session_start_time is a packaging date rather
    than the real acquisition time, as for the Visual Coding Neuropixels files).
    """
    # Extract age in days from NWB file subject.age field
    age_str = nwbfile.subject.age
    match = re.match(r'P(\d+)D', age_str)
    if not match:
        raise ValueError(f"Unable to parse age from NWB file. Expected format 'P<days>D', got '{age_str}'")

    age_in_days = int(match.group(1))

    # Calculate date of birth by subtracting age from acquisition date
    if acquisition_start_time is None:
        acquisition_start_time = nwbfile.session_start_time
    date_of_birth = (acquisition_start_time - timedelta(days=age_in_days)).date()

    return date_of_birth


# Visual Behavior Ophys imaging rigs (CAM2P.*/MESO.*) wrote the NWB session_start_time as
# the acquisition wall-clock in US/Pacific but labeled it "+00:00", so for those sessions the
# NWB session_start_time is wrong by the DST-aware Pacific offset (7 h PDT / 8 h PST). The
# session table's date_of_acquisition is the correct UTC. (This is the mirror image of the
# Visual Behavior Neuropixels case, where the CSV was mislabeled and the NWB was correct.)
_PACIFIC = ZoneInfo("America/Los_Angeles")


def get_session_start_time(nwbfile: NWBFile, session_info: pd.Series) -> datetime:
    """Return the session's true acquisition start time (UTC), failing loud on surprises.

    The session table ``date_of_acquisition`` is the authoritative UTC start. It normally
    agrees with the NWB ``session_start_time`` (behavior boxes and correctly-stamped
    sessions), in which case the NWB value is returned unchanged. On the imaging rigs
    (CAM2P.*/MESO.*) the NWB ``session_start_time`` is instead the acquisition wall-clock in
    US/Pacific mislabeled ``+00:00`` -- i.e. wrong by the DST-aware Pacific offset (7 h PDT /
    8 h PST) -- so the corrected UTC start (the CSV value) is returned instead. Because the
    NWB start time is wrong for those sessions, callers must re-anchor every NWB-relative
    time (data-stream / stimulus-epoch offsets, DOB) to the returned value.

    Raises
    ------
    ValueError
        If the NWB and CSV start times disagree by something other than the Pacific offset
        (an unexpected outcome that must be surfaced, not silently papered over).
    """
    csv_utc = datetime.fromisoformat(str(session_info['date_of_acquisition'])).astimezone(timezone.utc)
    nwb_utc = nwbfile.session_start_time.astimezone(timezone.utc)

    # Agree (behavior boxes / correctly-stamped imaging sessions): keep the NWB value.
    if abs((csv_utc - nwb_utc).total_seconds()) < 120:
        return nwbfile.session_start_time

    # Otherwise the NWB wall-clock must be US/Pacific-local mislabeled as UTC: reinterpreting
    # its digits as Pacific and converting to UTC must reproduce the CSV UTC start.
    nwb_wall_as_pacific = nwb_utc.replace(tzinfo=_PACIFIC).astimezone(timezone.utc)
    if abs((nwb_wall_as_pacific - csv_utc).total_seconds()) >= 120:
        raise ValueError(
            f"session_start_time mismatch not explained by the US/Pacific offset: session "
            f"table date_of_acquisition={csv_utc.isoformat()}, NWB "
            f"session_start_time={nwb_utc.isoformat()}."
        )
    # The CSV date_of_acquisition is the correct UTC acquisition start.
    return csv_utc


# Behavior-box rig names look like "BEH.G-Box6" (with a box number) or the bare cluster
# form "BEH.G" (without one). The single mesoscope rig "MESO.1" is shortened to "MESO".
# Everything else (CAM2P.3, NP.0, ...) is left as is.
_BEHAVIOR_BOX_RE = re.compile(r"^BEH\.([A-Za-z])(?:-Box(\d+))?$")
_MESOSCOPE_RE = re.compile(r"^MESO(\.\d+)?$")


def resolve_instrument_id(equipment_name: str) -> str:
    """Map a rig ``equipment_name`` to the instrument id used in the metadata.

    Behavior boxes get a compact ``[letter][number]`` id: ``"BEH.G-Box6" -> "G6"``. The
    bare cluster form (no box number) drops to just the letter: ``"BEH.G" -> "G"``. The
    single mesoscope rig is shortened: ``"MESO.1" -> "MESO"``. Other rigs (e.g.
    ``CAM2P.3``, ``NP.0``) are returned unchanged.

    Applied to both the generated Instrument's ``instrument_id`` and the Acquisition's
    ``instrument_id`` (via ``get_instrument_id``) so the two always match.
    """
    if _MESOSCOPE_RE.match(equipment_name):
        return "MESO"
    match = _BEHAVIOR_BOX_RE.match(equipment_name)
    if match is None:
        return equipment_name
    letter, number = match.group(1).upper(), match.group(2)
    return f"{letter}{number}" if number else letter


def get_instrument_id(nwbfile: NWBFile, session_info: pd.Series) -> str:
    """Get the instrument ID from the NWB file, cross-checked with the session info.

    The raw rig name (the NWB device name, cross-checked against
    ``session_info['equipment_name']``) is mapped through ``resolve_instrument_id`` so a
    behavior box like ``"BEH.G-Box6"`` becomes ``"G6"``; imaging/ephys rigs
    (``CAM2P.3``, ``MESO.1``, ``NP.0``) are unchanged. Matches the generated Instrument's id.
    """
    instrument = next(iter(nwbfile.devices))
    assert session_info['equipment_name'] == instrument, "instrument_id mismatch occurred"
    return resolve_instrument_id(instrument)


def get_total_reward_volume(nwbfile: NWBFile) -> float | None:
    if 'reward_volume' in nwbfile.trials.colnames:
        return float(nwbfile.trials['reward_volume'][:].sum())
    return None


def get_individual_reward_volume(nwbfile: NWBFile) -> float | None:
    """Smallest non-zero per-trial reward volume, or None if the session gave no rewards.

    A session may use more than one distinct reward volume; the smallest is recorded as
    the representative LickSpoutConfig.volume and the full set is documented separately
    via ``get_reward_volume_notes``.
    """
    if 'reward_volume' in nwbfile.trials.colnames:
        volumes = nwbfile.intervals['trials'].to_dataframe()['reward_volume'].unique()
        volumes = volumes[volumes > 0]
        if len(volumes) == 0:
            return None
        return float(volumes.min())

    return None


def get_reward_volume_notes(nwbfile: NWBFile) -> str | None:
    """Notes listing all reward volumes when a session used more than one, else None.

    ``get_individual_reward_volume`` records only the smallest, so this documents the
    full sorted set on the LickSpoutConfig (e.g. "Multiple reward volumes used: [0.005,
    0.007]"). Returns None for a session with a single (or no) non-zero reward volume.
    """
    if 'reward_volume' in nwbfile.trials.colnames:
        volumes = nwbfile.intervals['trials'].to_dataframe()['reward_volume'].unique()
        volumes = sorted(float(v) for v in volumes[volumes > 0])
        if len(volumes) > 1:
            return f"Multiple reward volumes used: {volumes}"
    return None


def get_curriculum_status(session_info: pd.Series):
    # NOTE - nwbfile.lab_meta_data['task_parameters'] also has several task parameters for behavior files that might be useful to record
    keys = ["experience_level", "image_set", "session_number", "prior_exposures_to_image_set",
            "prior_exposures_to_omissions", "prior_exposures_to_session_type"]
    curriculum_dict = {k: session_info[k] for k in keys if k in session_info.index}

    return json.dumps(curriculum_dict, cls=NumpyJsonEncoder)


# ---------------------------------------------------------------------------
# Neuropixels probe geometry for the Allen Brain Observatory rig.
#
# The six-probe rig places each probe (A-F) at a fixed position and orientation
# relative to bregma. These values are shared by both Neuropixels/ephys datasets
# (Visual Coding and Visual Behavior), whose NWB files name the probe devices
# probeA..probeF. Transcribed from the brain observatory probe-config reference.
# ---------------------------------------------------------------------------

# Per-probe orientation relative to the V1 reticle, in degrees.
PROBE_ROTATIONS = {
    'A': [-18.1947618, 18.45024358, -16.25274646],
    'B': [0, -39.08251859, -19.99997558],
    'C': [112.8691441, -71.24837276, -124.32351133],
    'D': [161.8052382, -18.45024358, -163.74725354],
    'E': [180, 39.08251859, -160.00002442],
    'F': [-67.1308559, 71.24837276, -55.67648867],
}

# Per-probe location relative to the V1 reticle (x, y, z, depth).
PROBE_OFFSETS = {
    'A': [0.770, -0.150, -1.335, 0],
    'B': [1.54, -0.149, -0.0004, 0],
    'C': [0.770, -0.149, 1.334, 0],
    'D': [-0.770, -0.149, 1.334, 0],
    'E': [-1.541, -0.149, 0.0004, 0],
    'F': [-0.77094, -0.14997, -1.33436, 0],
}

# Intended cortical visual area each probe targets, from the brain observatory
# probe-config reference. Each probe targets a single area; this is the *intended*
# target, not the structures actually recorded from (those stay in the NWB electrodes
# table and are not listed as targeted).
PROBE_TARGETED_STRUCTURES = {
    'A': CCFv3.by_acronym('VISam'),
    'B': CCFv3.by_acronym('VISpm'),
    'C': CCFv3.by_acronym('VISp'),
    'D': CCFv3.by_acronym('VISl'),
    'E': CCFv3.by_acronym('VISal'),
    'F': CCFv3.by_acronym('VISrl'),
}

# Reticle-to-bregma transform and insertion depth, shared by all probes.
RETICLE_ROTATION = [0.0, -23.11637603, 0.0]
RETICLE_OFFSET = [-2.777, -3.071, 0.607, 0]
PROBE_INSERTION_DEPTH = [0, -3.5, 0, 3.5]

# Ephys global (bregma-relative) coordinate system the probe transforms resolve into.
# Ephys-specific: the ophys datasets position imaging planes in their own frames and
# do not use this. See PROBE_COORDINATE_SYSTEM for the per-probe local frame. Uses the
# shared library BREGMA_RAS (bregma origin, ML/AP/SI = LR/PA/IS, mm); the library
# PROBE_RUFD has no equivalent, so PROBE_COORDINATE_SYSTEM stays hand-defined below.
EPHYS_GLOBAL_COORDINATE_SYSTEM = CoordinateSystemLibrary.BREGMA_RAS

# Probe-local coordinate system (tip origin) that each ProbeConfig is expressed in.
PROBE_COORDINATE_SYSTEM = CoordinateSystem(
    name="PROBE_RUFD",
    origin=Origin.TIP,
    axis_unit=SizeUnit.UM,  # standard unit for probe coordinates
    axes=[
        Axis(name=AxisName.X, direction=Direction.LR),
        Axis(name=AxisName.Y, direction=Direction.DU),
        Axis(name=AxisName.Z, direction=Direction.BF),
        Axis(name=AxisName.DEPTH, direction=Direction.UD),
    ],
)

PROBE_TRANSFORM_NOTES = """Six transformations define the position and orientation of the probe in the global coordinate space:

    Translation 1: moves the probe along the insertion axis to the approximate depth inside the brain
    Rotation 1: sets probe orientation relative to V1 reticle
    Rotation 2: flips probe by 90 degrees
    Translation 2: sets probe location relative to V1 reticle
    Rotation 3: rotates reticle-relative coordinates into bregma-relative coordinates
    Translation 3: translates reticle-relative coordinates into bregma-relative coordinates
    """


def probe_letter_from_device_name(device_name: str) -> str:
    """Extract the assembly letter from an NWB probe device name.

    The Neuropixels NWB files name probe devices ``probeA`` .. ``probeF``; this returns
    the upper-cased trailing identifier (e.g. ``"probeA" -> "A"``) used to look up the
    probe's fixed rig geometry and to match acquisition configs to instrument components.
    """
    match = re.fullmatch(r"probe[_\s-]?([A-Za-z0-9]+)", device_name, flags=re.IGNORECASE)
    if not match:
        raise ValueError(f"Cannot parse probe letter from device name {device_name!r}")
    return match.group(1).upper()


def build_probe_transform(letter: str) -> list:
    """Build the six-step transform placing probe ``letter`` in bregma-relative space.

    Mirrors the brain observatory probe-config reference: insertion depth, orientation
    relative to the V1 reticle, a 90-degree flip, the reticle-relative location, and the
    reticle-to-bregma rotation and translation. See ``PROBE_TRANSFORM_NOTES``.
    """
    return [
        Translation(translation=PROBE_INSERTION_DEPTH),  # depth along the insertion axis
        Rotation(angles=PROBE_ROTATIONS[letter], angles_unit=AngleUnit.DEG),  # orient vs V1 reticle
        Rotation(angles=[90, 0, 0], angles_unit=AngleUnit.DEG),  # flip probe by 90 degrees
        Translation(translation=PROBE_OFFSETS[letter]),  # locate vs V1 reticle
        Rotation(angles=RETICLE_ROTATION, angles_unit=AngleUnit.DEG),  # reticle -> bregma rotation
        Translation(translation=RETICLE_OFFSET),  # reticle -> bregma translation
    ]


def build_probe_config(nwbfile: NWBFile, device, device_name: str = None) -> ProbeConfig:
    """Build a ProbeConfig for a single EcephysProbe device.

    The targeted structure, coordinate system, and full position/orientation transform
    are the fixed brain-observatory rig geometry for the probe's letter (A-F), shared
    across the Neuropixels/ephys datasets. Each probe has a single targeted structure;
    structures the probe merely passed through stay in the NWB electrodes table and are
    not duplicated into ``other_targeted_structure``.

    Parameters
    ----------
    nwbfile : NWBFile
        The NWB file (accepted for interface symmetry; geometry is keyed by probe letter).
    device : Device
        The EcephysProbe device to build a config for; its name gives the probe letter.
    device_name : str, optional
        Name to record on the ProbeConfig. Defaults to ``device.name`` (the NWB
        device name); callers can override it to match an instrument component
        name (e.g. ``"ProbeA"``).

    Returns
    -------
    ProbeConfig
        The probe configuration for the given device.
    """
    letter = probe_letter_from_device_name(device.name)
    return ProbeConfig(
        device_name=device_name if device_name is not None else device.name,
        primary_targeted_structure=PROBE_TARGETED_STRUCTURES[letter],
        local_coordinate_system=PROBE_COORDINATE_SYSTEM,
        transform=build_probe_transform(letter),
        notes=PROBE_TRANSFORM_NOTES,
    )


def get_probe_configs(nwbfile: NWBFile) -> list[ProbeConfig]:
    """Get probe configurations for every EcephysProbe in the NWB file.

    Each probe's targeted structure and position/orientation are the fixed intended
    rig geometry keyed by probe letter (see ``build_probe_config``), not derived from
    the recorded electrode locations.

    Parameters
    ----------
    nwbfile : NWBFile
        The NWB file containing the probe devices

    Returns
    -------
    list[ProbeConfig]
        List of probe configuration objects for each probe in the file
    """
    return [
        build_probe_config(nwbfile, device)
        for device in nwbfile.devices.values()
        if device.__class__.__name__ == "EcephysProbe"
    ]


# Optotagging pulse-train parameters from the technical whitepaper. The NWB optotagging
# table records each pulse's start/stop time, ``duration`` and ``level`` (from which the
# per-pulse durations and light levels below are read directly), but NOT the inter-pulse
# interval or the raised-cosine ramp duration, so those two are taken from the whitepaper.
# The inter-pulse interval is cross-checked against the observed pulse-onset timing (see
# ``verify_optostimulation_timing``); the ramp duration has no counterpart in the file and
# cannot be verified.
OPTO_RAMP_DURATION_S = 0.0005
OPTO_INTER_PULSE_INTERVAL_S = 1.5
OPTO_INTER_PULSE_INTERVAL_DELAY_RANGE_S = (0.0, 0.5)
# Tolerance (s) added to the whitepaper [interval, interval + max jitter] window when
# checking the observed median pulse-onset gap; loose enough to avoid false positives from
# the mixed pulse durations, tight enough to catch a grossly different timing.
_OPTO_INTER_PULSE_INTERVAL_TOLERANCE_S = 0.5


def verify_optostimulation_timing(optogenetic_stimulation) -> None:
    """Warn if the whitepaper inter-pulse interval is inconsistent with the NWB opto timing.

    The optotagging table records each pulse's ``start_time``/``stop_time`` but not the
    inter-pulse interval itself, so ``OPTO_INTER_PULSE_INTERVAL_S`` (and its jitter range)
    come from the whitepaper. This cross-checks that value against the data: the median gap
    between consecutive pulse onsets should fall within the whitepaper window
    ``[interval, interval + max jitter]`` widened by ``_OPTO_INTER_PULSE_INTERVAL_TOLERANCE_S``.
    A larger deviation flags a session whose optotagging timing does not match the assumed
    protocol. The raised-cosine ramp duration (``OPTO_RAMP_DURATION_S``) is not recorded
    anywhere in the file and therefore cannot be verified.

    Parameters
    ----------
    optogenetic_stimulation : TimeIntervals
        The optogenetic stimulation time intervals from the NWB file.
    """
    starts = np.sort(optogenetic_stimulation.to_dataframe()['start_time'].to_numpy(dtype=float))
    if starts.size < 2:
        return  # need at least two pulses to measure an interval
    median_onset_gap = float(np.median(np.diff(starts)))
    low = OPTO_INTER_PULSE_INTERVAL_S - _OPTO_INTER_PULSE_INTERVAL_TOLERANCE_S
    high = (OPTO_INTER_PULSE_INTERVAL_S + OPTO_INTER_PULSE_INTERVAL_DELAY_RANGE_S[1]
            + _OPTO_INTER_PULSE_INTERVAL_TOLERANCE_S)
    if not (low <= median_onset_gap <= high):
        warnings.warn(
            f"Optotagging inter-pulse interval from the whitepaper "
            f"({OPTO_INTER_PULSE_INTERVAL_S}s + up to {OPTO_INTER_PULSE_INTERVAL_DELAY_RANGE_S[1]}s "
            f"jitter) is inconsistent with the NWB optotagging timing: observed median pulse-onset "
            f"gap is {median_onset_gap:.3f}s, outside the expected [{low:.3f}, {high:.3f}]s window."
        )


def get_optostimulation_parameters(optogenetic_stimulation) -> dict[str, OptotaggingStimulation]:
    """Extract optogenetic stimulation parameters from NWB optotagging data.

    The per-pulse durations and light levels are read from the NWB optotagging table; the
    ramp duration and inter-pulse interval are whitepaper constants (the table does not
    record them). ``verify_optostimulation_timing`` cross-checks the inter-pulse interval
    against the observed pulse-onset timing before the parameters are built.

    Parameters
    ----------
    optogenetic_stimulation : TimeIntervals
        The optogenetic stimulation time intervals from the NWB file

    Returns
    -------
    dict[str, OptotaggingStimulation]
        Dictionary mapping stimulus names to OptotaggingStimulation objects
    """
    # Cross-check the whitepaper inter-pulse interval against the recorded pulse timing.
    verify_optostimulation_timing(optogenetic_stimulation)
    opto_stimulation = dict()
    opto_df = optogenetic_stimulation.to_dataframe()
    for stimulus_name, df in opto_df.groupby('stimulus_name'):
        assert len(df['condition'].unique()) == 1, "Multiple pulse shapes found for stimulus_name"
        if 'pulse' in df['condition'].values[0].lower():
            pulse_shape = PulseShape.SQUARE  # TODO - double check if this is best descriptor for both slow and fast pulses
        elif 'cosine' in df['condition'].values[0].lower():
            pulse_shape = PulseShape.RAMP  # TODO - described as "raised cosine ramp" in whitepaper, could define new enum if needed
        else:
            raise ValueError(f"Unknown pulse shape in condition: {df['condition'].values[0]}")

        # get pulse duration and light levels used
        light_levels = sorted(df['level'].unique().tolist())
        pulse_durations = df['duration'].unique()

        # create custom OptotaggingStimulation model
        opto_stimulation[stimulus_name] = OptotaggingStimulation(
            stimulus_name=stimulus_name,
            pulse_shape=pulse_shape,
            pulse_durations=[np.round(p, 10) for p in pulse_durations],
            pulse_durations_unit=TimeUnit.S,
            ramp_duration=OPTO_RAMP_DURATION_S, # from technical whitepaper; not recorded in the NWB, cannot be verified
            ramp_duration_unit=TimeUnit.S,
            inter_pulse_interval=OPTO_INTER_PULSE_INTERVAL_S,  # whitepaper; cross-checked in verify_optostimulation_timing
            inter_pulse_interval_unit=TimeUnit.S,
            inter_pulse_interval_delay_range=OPTO_INTER_PULSE_INTERVAL_DELAY_RANGE_S,
            inter_pulse_interval_delay_range_unit=TimeUnit.S,
            light_levels=light_levels,
            condition_description=df['condition'].values[0],
        )

    return opto_stimulation


def warn_if_too_few_presentations(stimulus_type: str, num_presentations: int,
                                  expected_counts: dict) -> None:
    """Warn if a parameterized stimulus table has fewer presentations than expected.

    Some Allen Brain Observatory NWB files store truncated stimulus-presentation tables
    (e.g. the Visual Coding 2P ``static_gratings`` table has only a few rows instead of
    the full ~6000 individual grating presentations). ``expected_counts`` maps a
    ``stimulus_type`` to its expected presentation count; only stimuli present in the map
    are checked, so unknown/variable stimuli never warn.

    Parameters
    ----------
    stimulus_type : str
        The stimulus name (e.g. ``"static_gratings"``).
    num_presentations : int
        The number of presentation rows actually in the NWB stimulus table.
    expected_counts : dict
        Mapping of ``stimulus_type`` -> expected presentation count.
    """
    expected = expected_counts.get(stimulus_type)
    if expected is not None and num_presentations < expected:
        warnings.warn(
            f"Stimulus '{stimulus_type}' has {num_presentations} presentations in the NWB "
            f"file, fewer than the expected {expected} (the table may be truncated)."
        )


def get_visual_stimulation_parameters(table_key: str, intervals_table: pd.DataFrame) -> VisualStimulation:
    """Extract visual stimulation parameters from an intervals table.

    Parameters
    ----------
    table_key : str
        The name of the intervals table
    intervals_table : pd.DataFrame
        DataFrame containing the stimulus presentation intervals

    Returns
    -------
    VisualStimulation
        Visual stimulation object with extracted parameters
    """
    # TODO - determine if there are any other parameters to include or better units
    possible_parameters_and_units = {
        "orientation": "degrees",
        "spatial_frequency": "cycles/degree",
        "temporal_frequency": "Hz",
        "contrast": "percent",
        "duration": "S",
        "phase": None,
        "size": None,
        "image_name": None,
        "image_set": None,
        "stimulus_name": None,
        # Visual Coding ophys stores its single "epochs" table with a stimulus_type
        # column (the ordered list of stimuli in the session); keep it as a parameter.
        "stimulus_type": None,
        "stimulus_block": None,
        "color": None,
        "opacity": None,
        "mask": None,
        "speed": "degrees/second",
        "dir": "degrees",
        "coherence": "percent",
        "dotLife": None,
        "dotSize": None,
        "nDots": None,
        "fieldPos": None,
        "fieldShape": None,
        "fieldSize": None,
    }
    parameters = {}
    for param_key, param_unit in possible_parameters_and_units.items():
        if param_key in intervals_table.columns:
            parameter_values = intervals_table[param_key].unique().tolist()
            parameter_values = parameter_values[0] if len(parameter_values) == 1 else parameter_values
            parameters.update({param_key: parameter_values})
            if param_unit is not None:
                parameters.update({f"{param_key}_unit": param_unit})

    # The stimulus template names come from the 'stimulus_name' column when present
    # (ephys/behavior presentation tables). The Visual Coding ophys "epochs" table has
    # no such column, so there are no per-presentation template names.
    stimulus_template_name = (
        intervals_table['stimulus_name'].unique().tolist()
        if 'stimulus_name' in intervals_table.columns else []
    )
    visual_stimulation = VisualStimulation(
        stimulus_name=table_key,
        stimulus_parameters=parameters,
        stimulus_template_name=stimulus_template_name,
        notes=None,
    )
    return visual_stimulation


def convert_intervals_to_visual_stimulus_epoch(stimulus_name: str, table_key: str, intervals_table: pd.DataFrame,
                                               nwbfile: NWBFile, session_info: pd.Series = None,
                                               session_start_time: datetime = None,
                                               active_devices: list = None,
                                               extra_parameters: dict = None,
                                               stimulus_template_name: list = None,
                                               notes: str = None) -> StimulusEpoch:
    """Build a single visual ``StimulusEpoch`` from one stimulus-presentation intervals table.

    The epoch's modality is always ``StimulusModality.VISUAL`` and its parameters are
    extracted as a ``VisualStimulation`` (see ``get_visual_stimulation_parameters``), so
    this helper is only for visual stimulus tables. Non-visual stimulation (e.g.
    optogenetic tagging in the Neuropixels datasets) is built as its own epoch by the
    caller and does not go through here.

    Parameters
    ----------
    stimulus_name : str
        Name of the stimulus
    table_key : str
        Key for the intervals table
    intervals_table : pd.DataFrame
        DataFrame containing stimulus presentation intervals
    nwbfile : NWBFile
        The NWB file containing session information
    session_info : pd.DataFrame, optional
        DataFrame with session metadata (for visual behavior experiments)
    session_start_time : datetime, optional
        Absolute time to anchor the interval offsets to. Interval start/stop times
        are seconds relative to the session start; defaults to
        ``nwbfile.session_start_time``. Pass a corrected value when the file's
        session_start_time is a packaging date rather than the real acquisition time.
    active_devices : list, optional
        Device names to record as active for this epoch (must match instrument
        components). Defaults to ``["None"]`` when not provided; the Visual Coding
        ophys pipeline passes the stimulus monitor (e.g. ``["Stimulus Screen"]``).
    extra_parameters : dict, optional
        Additional stimulus parameters to merge into the ``VisualStimulation``
        ``stimulus_parameters`` (e.g. per-block metadata not present as columns of
        ``intervals_table``). Keys here take precedence over extracted ones.
    stimulus_template_name : list, optional
        Overrides the ``VisualStimulation`` ``stimulus_template_name`` (e.g. the
        referenced template for a natural-scenes/movie block). When ``None``, the value
        extracted from ``intervals_table`` is kept.
    notes : str, optional
        Free-text notes for the epoch (e.g. the stimulus description from the NWB).

    Returns
    -------
    StimulusEpoch
        Stimulus epoch object with extracted parameters
    """
    if session_start_time is None:
        session_start_time = nwbfile.session_start_time
    if active_devices is None:
        active_devices = ["None"]

    visual_stimulation = get_visual_stimulation_parameters(table_key, intervals_table).model_dump()
    if stimulus_template_name is not None:
        visual_stimulation['stimulus_template_name'] = stimulus_template_name
    if extra_parameters:
        visual_stimulation['stimulus_parameters'] = {
            **visual_stimulation['stimulus_parameters'],
            **extra_parameters,
        }

    return StimulusEpoch(
        stimulus_start_time=timedelta(seconds=intervals_table['start_time'].values[0]) + session_start_time,
        stimulus_end_time=timedelta(seconds=intervals_table['stop_time'].values[-1]) + session_start_time,
        stimulus_name=stimulus_name,
        # TODO - acquire additional info about the code used for this task - might not be available
        # will need to fill in with some type of information so we can use the Code.parameters field @Saskia
        code=Code(
            url="None",
            core_dependency=Software(
                name="PsychoPy",
                version=None,
            ),  # TODO - from whitepaper, add version if available @Saskia
            parameters=visual_stimulation,
        ),
        stimulus_modalities=[StimulusModality.VISUAL],
        notes=notes,
        active_devices=active_devices,
        performance_metrics=None,  # TODO - see if these are accessible anywhere?
        training_protocol_name=session_info["session_type"] if session_info is not None else None,  # e.g., "TRAINING_0_gratings_autorewards_15min"
        curriculum_status=get_curriculum_status(session_info) if session_info is not None else None,
    )


def serialized_dict(**kwargs) -> str:
    return json.dumps(dict(**kwargs), cls=NumpyJsonEncoder)


class NumpyJsonEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle NumPy data types."""
    def default(self, obj):
        if isinstance(obj, (np.integer, np.floating, np.ndarray)):
            return obj.tolist()
        return super().default(obj)