"""Generates Allen Brain Observatory Neuropixels Instrument"""

import json
import pandas as pd

from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo
from pynwb import NWBFile

from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.units import FrequencyUnit, SizeUnit
from aind_data_schema_models.devices import CameraTarget
from aind_data_schema_models.coordinates import AnatomicalRelative

from aind_data_schema.components.coordinates import (
    CoordinateSystemLibrary,
    Rotation,
    Translation,
)
from aind_data_schema.components.devices import (
    Camera,
    CameraAssembly,
    Cooling,
    Disc,
    EphysAssembly,
    EphysProbe,
    Filter,
    FilterType,
    Laser,
    Lens,
    LightEmittingDiode,
    Manipulator,
    Monitor,
    ProbeModel,
)
from aind_data_schema.components.identifiers import Software

from aind_data_schema.core.instrument import Instrument

from mindscope_to_nwb_zarr.aind_data_schema.utils import EPHYS_GLOBAL_COORDINATE_SYSTEM

# Six Neuropixels probe assemblies, one per letter A-F.
EPHYS_ASSEMBLY_LETTERS = "ABCDEF"

# Fallback instrument identifier, used only for sessions absent from the reference
# CSV (session 819701982). Sessions in the CSV use their actual rig id ("NP.1" or
# "NP.2"); see get_rig_id.
INSTRUMENT_ID = "NP"

# Per-session experiment start time, rig, and operator are not stored in the NWB
# files; they are transcribed from the original platform JSON files into this CSV.
EXPERIMENT_METADATA_PATH = (
    Path(__file__).parent.parent.parent.parent
    / "reference"
    / "neuropixels_vc_experiment_metadata.csv"
)

# Maps the NWB subject_id to the 6-digit mouse id. Keyed by the NWB subject_id, which
# is present for all 58 sessions (unlike the experiment CSV, which omits 819701982).
SUBJECT_MAPPING_PATH = (
    Path(__file__).parent.parent.parent.parent
    / "reference"
    / "visual_coding_ephys_subject_mapping.json"
)


@lru_cache(maxsize=1)
def _load_experiment_metadata() -> pd.DataFrame:
    """Load the per-session experiment metadata CSV, indexed by session id."""
    return pd.read_csv(EXPERIMENT_METADATA_PATH).set_index("session_id")


@lru_cache(maxsize=None)
def _load_subject_mapping(path: str) -> dict:
    """Load the NWB subject_id -> 6-digit mouse id mapping from the JSON (cached)."""
    with open(path, 'r') as f:
        return json.load(f)


def get_mouse_id(nwbfile: NWBFile, subject_mapping_path=None) -> str:
    """Return the 6-digit mouse id for a session's subject.

    Looked up from the subject mapping by the NWB ``subject_id``, which is present and
    correct for all 58 sessions (the experiment CSV omits 819701982). The 6-digit mouse
    id is what the AIND metadata service and the ethics review CSV are keyed by.
    """
    mapping = _load_subject_mapping(str(subject_mapping_path or SUBJECT_MAPPING_PATH))
    subject_id = str(nwbfile.subject.subject_id)
    mouse_id = mapping.get(subject_id)
    if mouse_id is None:
        raise KeyError(
            f"No mouse id found for NWB subject_id {subject_id!r} in {SUBJECT_MAPPING_PATH}"
        )
    return mouse_id


def get_experiment_metadata(session_id: int) -> pd.Series | None:
    """Return the reference experiment metadata row for a session.

    Returns ``None`` for sessions absent from the CSV. One session (819701982)
    has no entry, so callers must fall back to values derivable from the NWB file.
    """
    metadata = _load_experiment_metadata()
    if session_id not in metadata.index:
        return None
    return metadata.loc[session_id]


# The rig is at the Allen Institute in Seattle and the platform JSON timestamps are
# naive local time, so they are interpreted as US Pacific to make them tz-aware.
ACQUISITION_TIMEZONE = ZoneInfo("America/Los_Angeles")


def get_acquisition_start_time(nwbfile: NWBFile, session_info: pd.Series) -> datetime:
    """Real acquisition start time for a session.

    Read from the reference CSV ``ExperimentStartTime`` (naive Pacific -> tz-aware),
    since the NWB ``session_start_time`` is a packaging date, not the acquisition time.
    Falls back to ``nwbfile.session_start_time`` for session 819701982, which is absent
    from the CSV.
    """
    metadata = get_experiment_metadata(int(session_info['id']))
    if metadata is not None:
        return datetime.fromisoformat(metadata['ExperimentStartTime']).replace(tzinfo=ACQUISITION_TIMEZONE)
    return nwbfile.session_start_time


def get_rig_id(session_id: int) -> str:
    """Instrument/rig identifier for a session, e.g. "NP.1" or "NP.2".

    Read from the reference experiment metadata CSV so the instrument id matches the
    physical rig used. Falls back to the shared ``INSTRUMENT_ID`` ("NP") for session
    819701982, which is absent from the CSV.
    """
    metadata = get_experiment_metadata(session_id)
    if metadata is None:
        return INSTRUMENT_ID
    return metadata['rig_id']

# Optotagging light source. The rig was modified partway through data collection:
# sessions with id >= OPTOTAGGING_LASER_MIN_SESSION_ID used a 473 nm laser, while
# earlier sessions used a 465 nm LED. The instrument is reconstructed in two
# states accordingly. Both wavelengths are from the technical whitepaper.
OPTOTAGGING_LASER_NAME = "Optotagging Laser"
OPTOTAGGING_LED_NAME = "Optotagging LED"
OPTOTAGGING_LASER_WAVELENGTH = 473
OPTOTAGGING_LED_WAVELENGTH = 465
OPTOTAGGING_LASER_MIN_SESSION_ID = 789848216

# modification_date for the two rig states. The laser state is dated to the
# acquisition date of session 789848216 (the first laser session). The schema
# requires a modification_date, so the original (LED) state uses the earliest
# session acquisition date as a stand-in for the original build date.
LASER_RIG_MODIFICATION_DATE = date(2019, 1, 8)
LED_RIG_MODIFICATION_DATE = date(2018, 9, 25)


def uses_optotagging_laser(session_id: int) -> bool:
    """Whether a session used the 473 nm laser (vs. the 465 nm LED) for optotagging.

    Sessions with id >= ``OPTOTAGGING_LASER_MIN_SESSION_ID`` used a 473 nm laser;
    all earlier sessions used a 465 nm LED.
    """
    return int(session_id) >= OPTOTAGGING_LASER_MIN_SESSION_ID


def optotagging_device_name(session_id: int) -> str:
    """Instrument component name of the optotagging light source used by a session."""
    return OPTOTAGGING_LASER_NAME if uses_optotagging_laser(session_id) else OPTOTAGGING_LED_NAME


# Optotagging light source devices, one per rig state. Only one is present on a
# given instrument. The wavelength is a required property of the device.
optotagging_laser = Laser(
    name=OPTOTAGGING_LASER_NAME,
    manufacturer=Organization.UNKNOWN,
    wavelength=OPTOTAGGING_LASER_WAVELENGTH,
    wavelength_unit=SizeUnit.NM,
)
optotagging_led = LightEmittingDiode(
    name=OPTOTAGGING_LED_NAME,
    manufacturer=Organization.UNKNOWN,
    wavelength=OPTOTAGGING_LED_WAVELENGTH,
    wavelength_unit=SizeUnit.NM,
)


def ephys_assembly_name(letter: str) -> str:
    """Instrument component name for the ephys assembly of a given probe letter."""
    return f"Ephys Assembly {letter}"


def manipulator_name(letter: str) -> str:
    """Instrument component name for the manipulator of a given probe letter."""
    return f"{ephys_assembly_name(letter)} Manipulator"


def probe_name(letter: str) -> str:
    """Instrument component name for the probe of a given probe letter."""
    return f"Probe{letter}"


# Generate EphysAssembly objects A-F
ephys_assemblies = [
    EphysAssembly(
        name=ephys_assembly_name(letter),
        manipulator=Manipulator(
            name=manipulator_name(letter),
            manufacturer=Organization.NEW_SCALE_TECHNOLOGIES,
            model="M3-LS-1.8-6",
        ),
        probes=[
            EphysProbe(
                name=probe_name(letter),
                manufacturer=Organization.IMEC,
                probe_model=ProbeModel.NP1,
            )
        ],
    )
    for letter in EPHYS_ASSEMBLY_LETTERS
]

# Components shared by both rig states (everything except the optotagging light
# source, which differs between the LED and laser states).
base_components = [
    Monitor(
        name="Stimulus Screen",
        serial_number=None,
        manufacturer=Organization.ASUS,
        model="PA248Q",
        notes="viewing distance is from screen normal to bregma",
        refresh_rate=60,
        width=1920,
        height=1200,
        size_unit="pixel",
        viewing_distance=17,
        viewing_distance_unit="centimeter",
        relative_position=[AnatomicalRelative.ANTERIOR, AnatomicalRelative.RIGHT],
        contrast=30,
        brightness=50,
        coordinate_system=CoordinateSystemLibrary.SIPE_MONITOR_RTF,
        transform=[
            Rotation(angles=[45, 90, 0]),
            Translation(translation=[86.2, 118.6, 31.6]),
        ],
    ),
    Disc(
        name="MindScope Running Disc",
        manufacturer=Organization.AIND,
        radius=8.255,
        radius_unit="centimeter",
    ),
    CameraAssembly(
        name="Eye Camera Assembly",
        target=CameraTarget.EYE,
        relative_position=[AnatomicalRelative.RIGHT],
        camera=Camera(
            name="Eye Camera",
            detector_type="Camera",
            manufacturer=Organization.ALLIED,
            model="Mako G-032B",
            data_interface="Ethernet",
            cooling=Cooling.NO_COOLING,
            frame_rate=30.0,
            frame_rate_unit=FrequencyUnit.HZ,
            chroma="Monochrome",
        ),
        lens=Lens(
            name="Eye Camera Lens",
            manufacturer=Organization.INFINITY_PHOTO_OPTICAL,
            model="InfiniStix",
        ),
        filter=Filter(
            name="Eye Camera Filter",
            filter_type=FilterType.BANDPASS,
            manufacturer=Organization.SEMROCK,
            model="FF01-850/10-25",
            center_wavelength=850,
            wavelength_unit=SizeUnit.NM,
        ),
    ),
    Filter(
        name="Eye Camera Dichroic",
        filter_type=FilterType.DICHROIC,
        manufacturer=Organization.SEMROCK,
        model="FF750-SDi02-25x36",
    ),
    CameraAssembly(
        name="Body Camera Assembly",
        target=CameraTarget.BODY,
        relative_position=[AnatomicalRelative.LEFT, AnatomicalRelative.POSTERIOR],
        camera=Camera(
            name="Body Camera",
            detector_type="Camera",
            manufacturer=Organization.ALLIED,
            model="Mako G-032B",
            data_interface="Ethernet",
            cooling=Cooling.NO_COOLING,
            frame_rate=30.0,
            frame_rate_unit=FrequencyUnit.HZ,
            chroma="Monochrome",
        ),
        lens=Lens(
            name="Body Camera Lens",
            manufacturer=Organization.THORLABS,
            model="MVL8M23",
        ),
        filter=Filter(
            name="Body Camera Filter",
            manufacturer=Organization.SEMROCK,
            model="BSP01-785R-25",
            filter_type=FilterType.SHORTPASS,
            cut_off_wavelength=785,
            wavelength_unit=SizeUnit.NM,
        ),
    ),
    *ephys_assemblies,
]


def build_instrument(session_id: int) -> Instrument:
    """Build the instrument in the rig state used by the given session.

    The ``instrument_id`` is the actual rig ("NP.1" or "NP.2") from the reference
    CSV (``INSTRUMENT_ID`` for the one session absent from it). The optotagging light
    source and ``modification_date`` differ between the two rig states: sessions with
    id >= ``OPTOTAGGING_LASER_MIN_SESSION_ID`` get the 473 nm laser (dated to session
    789848216's acquisition), earlier sessions get the 465 nm LED. All other
    components are shared.
    """
    if uses_optotagging_laser(session_id):
        light_source = optotagging_laser
        modification_date = LASER_RIG_MODIFICATION_DATE
    else:
        light_source = optotagging_led
        modification_date = LED_RIG_MODIFICATION_DATE
    return Instrument(
        location="325",
        instrument_id=get_rig_id(session_id),
        modification_date=modification_date,
        coordinate_system=EPHYS_GLOBAL_COORDINATE_SYSTEM,  # bregma-relative frame the probe transforms resolve into
        modalities=[Modality.ECEPHYS, Modality.BEHAVIOR_VIDEOS],
        notes="Created several years posthoc from incomplete records. Much information is missing.",
        temperature_control=None,
        components=[*base_components, light_source],
    )

def generate_instrument(nwbfile=None, session_info=None) -> Instrument:
    """Return the AIND Instrument for a Visual Coding Neuropixels session.

    The ``instrument_id`` is the session's actual rig ("NP.1" or "NP.2", from the
    reference CSV; the shared ``INSTRUMENT_ID`` "NP" for session 819701982, which is
    absent from the CSV). Each rig is reconstructed in one of two states: the
    optotagging light source was changed from a 465 nm LED to a 473 nm laser starting
    with session 789848216. Both rig and state are selected from the session id
    (``session_info['id']``). The ``nwbfile`` parameter is accepted only for
    interface symmetry with the other ``generate_*`` functions.
    """
    return build_instrument(int(session_info["id"]))


if __name__ == "__main__":
    # Write both rig states for inspection.
    for label, session_id in [("led", OPTOTAGGING_LASER_MIN_SESSION_ID - 1),
                              ("laser", OPTOTAGGING_LASER_MIN_SESSION_ID)]:
        inst = build_instrument(session_id)
        serialized = inst.model_dump_json()
        deserialized = Instrument.model_validate_json(serialized)
        deserialized.write_standard_file(prefix=f"allen_brain_observatory_np_{label}")
