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
specification table rather than duplicated by hand.

Source: ``cam2p_{1..5}_{original,final}_instrument.py`` provided by the Allen
Institute (in the ``aind_metadata`` drop). This module reproduces those files
exactly; see ``scripts``-style verification in the project notes.

Selection of original vs. final for a given session is by acquisition date: a
session acquired on or after a rig's ``final_date`` uses the "final"
configuration, otherwise "original".
"""

import re
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

# Microscope variants. NOTE: the source files name the Scientifica microscope
# "Nikon 1" as well; this is preserved verbatim for fidelity to the provided
# instrument definitions.
_NIKON_MICROSCOPE = dict(
    name="Nikon 1",
    manufacturer=Organization.NIKON,
    model="A1R MP+",
)
_SCIENTIFICA_MICROSCOPE = dict(
    name="Nikon 1",
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
        coordinate_system=CoordinateSystemLibrary.SIPE_MONITOR_RTF,
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

    return Instrument(
        location="Unknown",
        instrument_id=rig_name,
        modification_date=modification_date,
        coordinate_system=CoordinateSystemLibrary.BREGMA_ARI,
        modalities=[Modality.POPHYS, Modality.BEHAVIOR_VIDEOS],
        notes=spec["notes"],
        temperature_control=None,
        components=[
            Microscope(**spec["microscope"]),
            *_build_shared_components()[:3],  # Laser, Objective, Detector
            _build_monitor(version),
            *_build_shared_components()[3:],  # Disc, cameras, dichroic
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
# Session -> rig resolution
# ---------------------------------------------------------------------------
#
# The rig used for a session is looked up from the Allen Institute mapping CSV
# (ophys_session_id -> rig_name). The conversion pipeline iterates ophys
# *experiments*, so we first recover the ophys_session_id for an experiment from
# its ``storage_directory``, which for the newer LIMS prod versions embeds an
# ``ophys_session_<id>`` path component, e.g.::
#
#     /external/neuralcoding/prod38/specimen_639389525/ophys_session_653077024/ophys_experiment_653123586/
#
# Older experiments store only ``ophys_experiment_<id>`` (no session id), and
# there is no local/public way to recover their ophys_session_id, so those
# experiments cannot be assigned a rig and are skipped (return ``None``).

# Mapping CSV bundled in the repo (also holds screen centers, added later).
_DEFAULT_RIG_CSV = Path(__file__).resolve().parents[3] / "reference" / "ophys_session_screen_centers.csv"

_OPHYS_SESSION_RE = re.compile(r"ophys_session_(\d+)")


def extract_ophys_session_id(storage_directory) -> int | None:
    """Recover the ophys_session_id from an experiment's ``storage_directory``.

    Returns ``None`` if the path has no ``ophys_session_<id>`` component (older
    LIMS prod versions), which means the session id is not recoverable locally.
    """
    if not storage_directory or not isinstance(storage_directory, str):
        return None
    match = _OPHYS_SESSION_RE.search(storage_directory)
    return int(match.group(1)) if match else None


@lru_cache(maxsize=None)
def _load_session_to_rig(csv_path: str) -> dict:
    """Load the ophys_session_id -> rig_name mapping from the CSV (cached)."""
    df = pd.read_csv(csv_path, usecols=["ophys_session_id", "rig_name"])
    df = df.dropna(subset=["ophys_session_id"])
    return dict(zip(df["ophys_session_id"].astype(int), df["rig_name"]))


def rig_for_experiment(session_info, rig_csv_path=None) -> str | None:
    """Look up the rig_name for an ophys experiment, or ``None`` if unresolved.

    Parameters
    ----------
    session_info : Mapping
        A row of the ophys experiment metadata (e.g. a ``pandas.Series``); must
        provide ``storage_directory``.
    rig_csv_path : str | Path, optional
        Path to the session->rig mapping CSV. Defaults to the bundled copy.
    """
    session_id = extract_ophys_session_id(session_info.get("storage_directory"))
    if session_id is None:
        return None
    mapping = _load_session_to_rig(str(rig_csv_path or _DEFAULT_RIG_CSV))
    return mapping.get(session_id)


def generate_instrument(nwbfile, session_info, rig_csv_path=None) -> Instrument | None:
    """Generate the AIND Instrument for an ophys experiment.

    Resolves the rig from the session->rig mapping CSV (via the experiment's
    ``storage_directory``) and selects the original/final configuration by
    acquisition date. Returns ``None`` if the rig cannot be resolved or has no
    instrument definition, so the caller can skip those experiments.

    Parameters
    ----------
    nwbfile : NWBFile
        The opened NWB file (kept for interface symmetry with the other
        ``generate_*`` functions; the acquisition date is taken from
        ``session_info``).
    session_info : pd.Series
        Session metadata row from the ophys experiment metadata.
    rig_csv_path : str | Path, optional
        Path to the session->rig mapping CSV. Defaults to the bundled copy.
    """
    rig_name = rig_for_experiment(session_info, rig_csv_path)
    if rig_name is None:
        # ophys_session_id not recoverable from storage_directory (older data).
        return None
    if rig_name not in RIG_SPECS:
        # e.g. CAM2P.6 / 3Pscope / DS.1 -- no instrument definition provided.
        return None

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
