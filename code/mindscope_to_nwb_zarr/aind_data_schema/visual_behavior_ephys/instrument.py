"""Generates AIND Instrument metadata for the Visual Behavior Neuropixels dataset.

Two rig families appear in this dataset, selected per session by the ``equipment_name``
recorded in the session tables (behavior_sessions.csv / ecephys_sessions.csv):

* ``NP.*``   -> the Neuropixels rig (ecephys sessions, and behavior-only habituation run
                on the same physical rig)
* ``BEH.*``  -> a MindScope behavior training box (behavior-only training sessions)

Selection is by *physical rig* (``equipment_name``), not by whether ephys was recorded: a
behavior-only habituation session run on an NP rig still gets the NP instrument. This
mirrors the Visual Behavior Ophys module.

The NP instrument is the Visual Coding Neuropixels instrument (``visual_coding_ephys``)
reused wholesale -- same monitor, running disc, eye/body cameras, six Neuropixels ephys
assemblies, and 473 nm optotagging laser -- plus a **reward/lick spout** (the Visual
Behavior task is reward-driven; Visual Coding was passive and had none) and the
``BEHAVIOR`` modality. The reward spout is the shared Visual Behavior Ophys component. The
BEH box is the Visual Behavior Ophys behavior-box instrument, which already carries the
same reward spout.

All Visual Behavior Neuropixels acquisitions fall in the laser era of the Visual Coding
rig (session ids far above the LED->laser cutoff), so the 473 nm optotagging laser is
always used (never the 465 nm LED).
"""

from datetime import date
from pathlib import Path

import pandas as pd

from aind_data_schema_models.modalities import Modality
from aind_data_schema.core.instrument import Instrument

from mindscope_to_nwb_zarr.aind_data_schema.utils import EPHYS_GLOBAL_COORDINATE_SYSTEM, first_use_date
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.instrument import (
    base_components,        # monitor, running disc, eye/body cameras, 6 ephys assemblies
    optotagging_laser,      # 473 nm optotagging laser (the laser-era light source)
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.instrument import (
    build_behavior_instrument,
    _build_reward_spout,
)


# Fallback modification date, used only if build_np_instrument is called directly without
# one; generate_instrument overrides it with the rig's first-use date (see below).
_NP_MODIFICATION_DATE = date(2020, 8, 17)
_POSTHOC_NOTE = "Created several years posthoc from incomplete records. Much information is missing."

# Session tables used to date each instrument by the rig's first day of use. parents[4] is
# the repo root (this file is code/mindscope_to_nwb_zarr/aind_data_schema/visual_behavior_ephys/).
_PROJECT_METADATA = Path(__file__).resolve().parents[4] / "data" / "visual-behavior-neuropixels" / "project_metadata"
_SESSION_TABLES = (
    _PROJECT_METADATA / "behavior_sessions.csv",
    _PROJECT_METADATA / "ecephys_sessions.csv",
)


def build_np_instrument(equipment_name: str, modification_date: date = _NP_MODIFICATION_DATE) -> Instrument:
    """Build the Neuropixels-rig Instrument for a session.

    Identical to the Visual Coding Neuropixels instrument (``base_components`` +
    473 nm optotagging laser) with a reward/lick spout added and the ``BEHAVIOR``
    modality included, since the Visual Behavior task is reward-driven. ``instrument_id``
    is the exact ``equipment_name`` ("NP.0" / "NP.1").
    """
    return Instrument(
        location="Unknown",
        instrument_id=equipment_name,
        modification_date=modification_date,
        global_coordinate_system=EPHYS_GLOBAL_COORDINATE_SYSTEM,  # bregma-relative frame the probe transforms resolve into
        modalities=[Modality.ECEPHYS, Modality.BEHAVIOR, Modality.BEHAVIOR_VIDEOS],
        notes=_POSTHOC_NOTE,
        components=[*base_components, optotagging_laser, _build_reward_spout()],
    )


def generate_instrument(session_info: pd.Series) -> Instrument:
    """Build the correct Instrument for a session based on its ``equipment_name``.

    Works for both behavior-only rows (from ``behavior_sessions.csv``) and ecephys rows
    (from ``ecephys_sessions.csv``); both carry ``equipment_name``. ``NP.*`` rigs get the
    Neuropixels instrument (with lick spout); ``BEH.*`` boxes get the Visual Behavior Ophys
    behavior-box instrument (which already has a lick spout).

    Raises
    ------
    ValueError
        If ``equipment_name`` does not match a known rig family.
    """
    equipment_name = str(session_info["equipment_name"])
    # modification_date = the first day this rig was used in the Visual Behavior Neuropixels
    # dataset (a rig may be used earlier in other datasets; the date is per-experiment).
    modification_date = first_use_date(equipment_name, _SESSION_TABLES)

    if equipment_name.startswith("NP"):
        return build_np_instrument(equipment_name, modification_date)
    if equipment_name.startswith("BEH"):
        return build_behavior_instrument(equipment_name, modification_date)

    raise ValueError(
        f"No instrument definition for equipment_name '{equipment_name}'. "
        "Expected a name starting with 'NP' or 'BEH'."
    )
