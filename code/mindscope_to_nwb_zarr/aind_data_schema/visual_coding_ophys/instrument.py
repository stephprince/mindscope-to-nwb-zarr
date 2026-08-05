"""Generates AIND Instrument metadata for Visual Coding 2P sessions.

Each Visual Coding 2P session was acquired on one of the CAM2P rigs (CAM2P.1 -
CAM2P.5). The rig hardware was reconstructed posthoc from incomplete records by
the Allen Institute and provided as per-rig instrument definitions. For each rig
there are two configurations:

- "original": the rig as originally built.
- "final":    the rig after the stimulus monitor was repositioned (a rotation
              and translation of the Monitor). The repositioning happened on a
              per-rig date (the ``final`` ``modification_date`` below).

The two configurations differ only in the Monitor ``relative_position`` and
``transform`` (and the instrument ``modification_date``). Rigs also differ from
one another only in the microscope (Nikon vs. Scientifica) and a one-line note.
Everything else is shared, so the full set of instruments is built from a small
specification table.

Source: ``cam2p_{1..5}_{original,final}_instrument.py`` provided by the Allen
Institute.

Selection of original vs. final for a given session is by acquisition date: a
session acquired on or after a rig's ``final_date`` uses the "final"
configuration, otherwise "original".
"""

from datetime import date, datetime
from functools import lru_cache
from pathlib import Path

import pandas as pd

from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.units import FrequencyUnit, SizeUnit, UnitlessUnit
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
    DataInterface,
    Detector,
    Disc,
    Filter,
    FilterType,
    Laser,
    Lens,
    Microscope,
    Monitor,
    Objective,
)
from aind_data_schema.core.instrument import Instrument


# ---------------------------------------------------------------------------
# Specification tables
# ---------------------------------------------------------------------------

# Note common to all rigs; the Nikon rigs (CAM2P.1, CAM2P.2) add a sentence
# about the proprietary Nikon detector.
_BASE_NOTE = (
    "Created several years posthoc from incomplete records. Much information is missing."
)
_NIKON_NOTE = _BASE_NOTE + " Detector was proprietary part of Nikon."

# Microscope variants (manufacturer/model only). The microscope device *name* is
# per-rig (see RIG_MICROSCOPE_NAMES) and assigned in build_instrument; it is
# referenced by the acquisition's ImagingConfig.device_name so the config points
# at the instrument's microscope device.
_NIKON_MICROSCOPE = dict(
    manufacturer=Organization.NIKON,
    model="A1R MP+",
)
_SCIENTIFICA_MICROSCOPE = dict(
    manufacturer=Organization.SCIENTIFICA,
    model="Vivoscope",
)

# Per-rig specification: which microscope/note, and the original/final dates.
# ``final_date`` is the date the monitor was repositioned and is also the
# cutoff used to select the "final" configuration for a session.
RIG_SPECS = {
    "CAM2P.1": dict(
        microscope=_NIKON_MICROSCOPE,
        notes=_NIKON_NOTE,
        original_date=date(2015, 3, 31),
        final_date=date(2016, 10, 11),
    ),
    "CAM2P.2": dict(
        microscope=_NIKON_MICROSCOPE,
        notes=_NIKON_NOTE,
        original_date=date(2015, 4, 10),
        final_date=date(2016, 10, 11),
    ),
    "CAM2P.3": dict(
        microscope=_SCIENTIFICA_MICROSCOPE,
        notes=_BASE_NOTE,
        original_date=date(2016, 3, 10),
        final_date=date(2016, 10, 12),
    ),
    "CAM2P.4": dict(
        microscope=_SCIENTIFICA_MICROSCOPE,
        notes=_BASE_NOTE,
        original_date=date(2016, 6, 17),
        final_date=date(2016, 8, 9),
    ),
    "CAM2P.5": dict(
        microscope=_SCIENTIFICA_MICROSCOPE,
        notes=_BASE_NOTE,
        original_date=date(2016, 6, 24),
        final_date=date(2016, 10, 11),
    ),
}

# Per-rig microscope device name. The instrument_id stays the internal rig id
# ("CAM2P.N"); the microscope device is named after the rig's microscope (Nikon
# for CAM2P.1/.2, Scientifica for CAM2P.3/.4/.5). The acquisition's
# ImagingConfig.device_name matches this name.
RIG_MICROSCOPE_NAMES = {
    "CAM2P.1": "Nikon 1",
    "CAM2P.2": "Nikon 2",
    "CAM2P.3": "Scientifica 1",
    "CAM2P.4": "Scientifica 2",
    "CAM2P.5": "Scientifica 3",
}

# Monitor configuration per version. The only hardware difference between the
# "original" and "final" rigs is the position/orientation of the stimulus
# monitor relative to the mouse.
_MONITOR_VERSIONS = {
    "original": dict(
        relative_position=[AnatomicalRelative.RIGHT],
        transform=[Rotation(angles=[0, 90, 0]), Translation(translation=[0, 170, 0])],
    ),
    "final": dict(
        relative_position=[AnatomicalRelative.ANTERIOR, AnatomicalRelative.RIGHT],
        transform=[Rotation(angles=[45, 90, 0]), Translation(translation=[86.2, 118.6, 31.6])],
    ),
}

VERSIONS = tuple(_MONITOR_VERSIONS.keys())


# ---------------------------------------------------------------------------
# Component builders
# ---------------------------------------------------------------------------

def _build_monitor(version: str) -> Monitor:
    """Build the stimulus Monitor for the given configuration version."""
    monitor_version = _MONITOR_VERSIONS[version]
    return Monitor(
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
        relative_position=monitor_version["relative_position"],
        contrast=30,
        contrast_unit=UnitlessUnit.PERCENT,
        brightness=50,
        brightness_unit=UnitlessUnit.PERCENT,
        local_coordinate_system=CoordinateSystemLibrary.SIPE_MONITOR_RTF,
        transform=monitor_version["transform"],
    )


def _build_shared_components() -> list:
    """Build the components shared by every rig and version (everything except
    the microscope and the monitor)."""
    return [
        Laser(
            name="Ti-Saph",
            wavelength=910,
            wavelength_unit=SizeUnit.NM,
            manufacturer=Organization.COHERENT_SCIENTIFIC,
            model="Chameleon Vision",
        ),
        Objective(
            name="Nikon 16x",
            numerical_aperture=0.8,
            magnification=16,
            manufacturer=Organization.NIKON,
            immersion="water",
            model="N16XLWD-PF",
        ),
        Detector(
            name="PMT",
            manufacturer=Organization.UNKNOWN,
            detector_type="Photomultiplier Tube",
            data_interface=DataInterface.OTHER,
            notes="Unknown data interface",
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
    ]


# ---------------------------------------------------------------------------
# Instrument builders / selection
# ---------------------------------------------------------------------------

def build_instrument(rig_name: str, version: str) -> Instrument:
    """Build the Instrument for a given rig and configuration version.

    Parameters
    ----------
    rig_name : str
        One of the keys of ``RIG_SPECS`` (e.g. ``"CAM2P.1"``).
    version : str
        ``"original"`` or ``"final"``.

    Returns
    -------
    Instrument
        The fully-populated AIND Instrument model for that rig/version.
    """
    if rig_name not in RIG_SPECS:
        raise ValueError(
            f"No instrument definition for rig '{rig_name}'. "
            f"Known rigs: {sorted(RIG_SPECS)}"
        )
    if version not in _MONITOR_VERSIONS:
        raise ValueError(f"Unknown version '{version}'. Expected one of {VERSIONS}.")

    spec = RIG_SPECS[rig_name]
    modification_date = spec["original_date"] if version == "original" else spec["final_date"]

    # Laser, Objective, Detector, then (after the monitor) Disc, cameras, dichroic.
    shared_components = _build_shared_components()

    return Instrument(
        location="Unknown",
        instrument_id=rig_name,
        modification_date=modification_date,
        global_coordinate_system=CoordinateSystemLibrary.BREGMA_ARI,
        modalities=[Modality.POPHYS, Modality.BEHAVIOR_VIDEOS],
        notes=spec["notes"],
        temperature_control=None,
        components=[
            Microscope(name=RIG_MICROSCOPE_NAMES[rig_name], **spec["microscope"]),
            *shared_components[:3],  # Laser, Objective, Detector
            _build_monitor(version),
            *shared_components[3:],  # Disc, cameras, dichroic
        ],
    )


def select_version(rig_name: str, acquisition_date: date) -> str:
    """Select "original" or "final" for a session based on its acquisition date.

    A session acquired on or after the rig's ``final_date`` (the date the
    stimulus monitor was repositioned) uses the "final" configuration;
    otherwise it uses "original".
    """
    if rig_name not in RIG_SPECS:
        raise ValueError(
            f"No instrument definition for rig '{rig_name}'. "
            f"Known rigs: {sorted(RIG_SPECS)}"
        )
    if isinstance(acquisition_date, datetime):
        acquisition_date = acquisition_date.date()
    final_date = RIG_SPECS[rig_name]["final_date"]
    return "final" if acquisition_date >= final_date else "original"


def get_instrument_for_session(rig_name: str, acquisition_date: date) -> Instrument:
    """Build the correct Instrument for a session given its rig and acquisition date."""
    return build_instrument(rig_name, select_version(rig_name, acquisition_date))


# ---------------------------------------------------------------------------
# Experiment -> rig resolution
# ---------------------------------------------------------------------------
#
# The rig used for a session is looked up from the Allen Institute mapping CSV,
# keyed by ``ophys_experiment_id`` -- the conversion pipeline's iteration key
# (``session_info['id']``, equal to ``nwbfile.session_id``). Every experiment in the
# dataset is present in the CSV; a missing one raises (see the functions below).

# Mapping CSV bundled in the repo (also holds ophys_session_id and screen centers).
_DEFAULT_RIG_CSV = Path(__file__).resolve().parents[3] / "reference" / "ophys_session_experiment_screen_centers.csv"


@lru_cache(maxsize=None)
def _load_experiment_to_rig() -> dict:
    """Load the ophys_experiment_id -> rig_name mapping from the CSV (cached)."""
    df = pd.read_csv(_DEFAULT_RIG_CSV, usecols=["ophys_experiment_id", "rig_name"])
    df = df.dropna(subset=["ophys_experiment_id", "rig_name"])
    return dict(zip(df["ophys_experiment_id"].astype(int), df["rig_name"]))


def rig_for_experiment(session_info) -> str:
    """Look up the rig_name (e.g. "CAM2P.1") for an ophys experiment.

    ``session_info`` is a row of the ophys experiment metadata (e.g. a
    ``pandas.Series``); it must provide ``id`` (the ophys_experiment_id). Every
    experiment in the dataset is in the CSV.

    Raises
    ------
    ValueError
        If the experiment has no rig in the CSV.
    """
    mapping = _load_experiment_to_rig()
    experiment_id = int(session_info['id'])
    if experiment_id not in mapping:
        raise ValueError(
            f"No rig found for ophys_experiment_id {experiment_id} in the CSV."
        )
    return mapping[experiment_id]


@lru_cache(maxsize=None)
def _load_experiment_to_session_id() -> dict:
    """Load the ophys_experiment_id -> ophys_session_id mapping from the CSV (cached)."""
    df = pd.read_csv(_DEFAULT_RIG_CSV, usecols=["ophys_experiment_id", "ophys_session_id"])
    df = df.dropna(subset=["ophys_experiment_id", "ophys_session_id"])
    return dict(zip(df["ophys_experiment_id"].astype(int), df["ophys_session_id"].astype(int)))


def ophys_session_id_for_experiment(session_info) -> int:
    """Look up the ophys_session_id for an ophys experiment from the CSV.

    Every experiment in the dataset has an ophys_session_id in the CSV.

    Raises
    ------
    ValueError
        If the experiment has no ophys_session_id in the CSV.
    """
    mapping = _load_experiment_to_session_id()
    experiment_id = int(session_info['id'])
    if experiment_id not in mapping:
        raise ValueError(
            f"No ophys_session_id found for ophys_experiment_id {experiment_id} in the CSV."
        )
    return mapping[experiment_id]


def microscope_name_for_experiment(session_info) -> str:
    """Resolve the microscope device name for an experiment's rig.

    Maps the rig (see :func:`rig_for_experiment`) to its microscope device name
    (e.g. ``"CAM2P.1" -> "Nikon 1"``), matched by the acquisition's
    ImagingConfig.device_name.
    """
    return RIG_MICROSCOPE_NAMES[rig_for_experiment(session_info)]


def generate_instrument(session_info) -> Instrument:
    """Generate the AIND Instrument for an ophys experiment.

    Resolves the rig from the experiment->rig mapping CSV (by ophys_experiment_id)
    and selects the original/final configuration by acquisition date. Every
    experiment in the dataset resolves to a rig with an instrument definition.

    Parameters
    ----------
    session_info : pd.Series
        Session metadata row from the ophys experiment metadata.

    Raises
    ------
    ValueError
        If the experiment's rig is not in the CSV, or resolves to a rig with no
        instrument definition. This fails the session loudly so the gap is surfaced.
    """
    experiment_id = int(session_info["id"])
    rig_name = rig_for_experiment(session_info)
    if rig_name not in RIG_SPECS:
        # Rigs in the CSV without an instrument definition: CAM2P.6, 3Pscope, DS.1, MESO.1/.2.
        raise ValueError(
            f"ophys_experiment_id {experiment_id} is on rig {rig_name!r}, which has no "
            f"instrument definition (known rigs: {sorted(RIG_SPECS)})."
        )

    acquisition_date = pd.Timestamp(session_info["date_of_acquisition"]).date()
    return get_instrument_for_session(rig_name, acquisition_date)


if __name__ == "__main__":
    # Build every rig/version and confirm each round-trips through serialization.
    for _rig in RIG_SPECS:
        for _version in VERSIONS:
            inst = build_instrument(_rig, _version)
            serialized = inst.model_dump_json()
            Instrument.model_validate_json(serialized)
            print(f"OK  {_rig:<8} {_version:<9} modification_date={inst.modification_date}")
