"""Backfill the AllenSDK-returned per-unit metadata onto the Visual Behavior Neuropixels units table.

The AllenSDK VBN units table (``VisualBehaviorNeuropixelsProjectCache.get_unit_table``) is the
released per-unit table: the spike-sorting metrics that are already in the NWB ``units`` table
**plus** per-unit CCF coordinates, brain structure (acronym + numeric id), probe/channel
identifiers, channel geometry, and ``waveform_halfwidth``. The source NWB units table carries only
the metrics and ``peak_channel_id``; this module adds the remaining AllenSDK columns so the archived
units table contains all of the same information the AllenSDK returns -- even where it is also
derivable from the ``electrodes`` table in the same file.

Why the values come from the released ``units.csv`` rather than the in-file electrodes table: most of
the missing columns *are* on the electrodes table (``x``/``y``/``z`` = AP/DV/LR, ``location`` =
structure acronym, ``probe_*_position``, ``valid_data``, ``probe_id``), but three pieces are not
faithfully reconstructable from the NWB -- the numeric ``structure_id`` and ``waveform_halfwidth``
are absent from the NWB entirely, and the AllenSDK ``structure_acronym`` is layer-stripped. So all the
added columns are taken from the released ``units.csv`` (keyed by ``unit_id`` == the NWB ``units.id``)
to reproduce the AllenSDK table exactly. This mirrors the Visual Coding Neuropixels pipeline, which
likewise joins a released per-unit CSV onto the units table (see
``visual_coding_ephys/_units_analysis_metrics.py``).

Column names match the AllenSDK VBN units table verbatim (e.g. ``structure_acronym`` /
``structure_id`` -- note VBN uses the un-prefixed names, unlike Visual Coding's
``ecephys_structure_*``). The metric columns the NWB already has are left untouched.

This module also fixes the *electrodes* table CCF coordinates: the source NWB already carries the
correct ``x``/``y``/``z`` (AP/DV/LR) on registered channels (verified equal to the released
``channels.csv`` on every channel across the dataset), but stores channels that were never
registered to the CCF as ``(0, 0, 0)`` on all three axes rather than the Visual Coding Neuropixels
``-1000`` sentinel (7 of the 153 ecephys sessions are entirely unregistered). Because the NWB is
already correct on registered channels, no external re-source is needed:
``null_unregistered_electrode_ccf_coordinates`` maps that all-axes-zero sentinel to NaN in place so
those channels are not stored as a false CCF-origin location.
"""
import warnings
from functools import lru_cache

import numpy as np
import pandas as pd

# Public regional HTTPS front of the released VBN project metadata (same prefix as the
# behavior_sessions.csv / ecephys_sessions.csv session tables).
UNITS_CSV_URL = (
    "https://visual-behavior-neuropixels-data.s3.us-west-2.amazonaws.com/"
    "visual-behavior-neuropixels/project_metadata/units.csv"
)
_UNIT_ID_KEY = "unit_id"

# The AllenSDK units-table columns that are NOT already in the source NWB units table, with
# descriptions. Adding these (and nothing else) makes the archived units table carry the full
# AllenSDK units-table information. Order is the order columns are appended.
_ADDED_COLUMN_DESCRIPTIONS = {
    "anterior_posterior_ccf_coordinate": (
        "Anterior-posterior position of this unit's peak channel in the Allen CCFv3 (microns); "
        "NaN if the channel is out of brain or unregistered."
    ),
    "dorsal_ventral_ccf_coordinate": (
        "Dorsal-ventral position of this unit's peak channel in the Allen CCFv3 (microns); "
        "NaN if the channel is out of brain or unregistered."
    ),
    "left_right_ccf_coordinate": (
        "Left-right (medial-lateral) position of this unit's peak channel in the Allen CCFv3 "
        "(microns); NaN if the channel is out of brain or unregistered."
    ),
    "structure_acronym": (
        "Acronym of the Allen CCFv3 brain structure containing this unit's peak channel (the "
        "unit's recorded brain area, with layer substructure stripped as in the AllenSDK units "
        "table); empty if the channel is out of brain or unassigned."
    ),
    "structure_id": (
        "Allen CCFv3 structure ID of the brain region containing this unit's peak channel "
        "(numeric counterpart of 'structure_acronym'); NaN if out of brain or unassigned."
    ),
    "probe_vertical_position": (
        "Distance (microns) from the probe tip to this unit's peak channel along the probe."
    ),
    "probe_horizontal_position": (
        "Horizontal (across-probe) position (microns) of this unit's peak channel."
    ),
    "valid_data": "Whether this unit's peak channel is flagged as carrying valid data.",
    "ecephys_probe_id": "Identifier of the Neuropixels probe that recorded this unit.",
    "ecephys_channel_id": (
        "Identifier of this unit's peak channel (equal to the units table's 'peak_channel_id'; "
        "the AllenSDK units table's name for it)."
    ),
    "ecephys_session_id": (
        "Identifier of the ecephys session (the same for every unit in this file; included to "
        "match the AllenSDK units table)."
    ),
    "waveform_halfwidth": (
        "Width (ms) of this unit's mean waveform at half the trough depth. Present in the AllenSDK "
        "units table but absent from the source NWB units table."
    ),
}
_PROVENANCE = (
    " Source: Allen Brain Observatory Visual Behavior Neuropixels released units.csv, "
    "joined on unit_id (== the NWB units id)."
)


@lru_cache(maxsize=1)
def _load_units_table(url: str = UNITS_CSV_URL) -> pd.DataFrame:
    """Load the released dataset-wide ``units.csv``, indexed by ``unit_id``.

    Cached so the ~130 MB CSV is fetched at most once per conversion. Callers only ``.loc``
    (never mutate) the returned frame, so sharing is safe.
    """
    print(f"Loading VBN units table from {url} ...")
    return pd.read_csv(url).set_index(_UNIT_ID_KEY)


def add_allensdk_unit_columns(nwbfile, url: str = UNITS_CSV_URL) -> int:
    """Add the AllenSDK units-table columns to ``nwbfile.units``, joined on the unit id.

    No-op (returns 0) for behavior-only sessions, which have no units table. Raises if any NWB
    unit id is absent from the released ``units.csv`` (which is a strict superset of every
    session's units), so a broken join key fails loudly instead of silently NaN-filling. Returns
    the number of columns added.
    """
    if nwbfile.units is None:
        return 0

    units_csv = _load_units_table(url)
    missing_cols = [c for c in _ADDED_COLUMN_DESCRIPTIONS if c not in units_csv.columns]
    if missing_cols:
        raise RuntimeError(f"units.csv is missing expected column(s): {missing_cols}.")

    unit_ids = np.asarray(nwbfile.units.id[:])
    absent = set(unit_ids.tolist()) - set(units_csv.index.tolist())
    if absent:
        raise RuntimeError(
            f"{len(absent)} of {len(unit_ids)} NWB unit ids are absent from the released "
            f"units.csv (e.g. {sorted(absent)[:3]}); the unit_id join key may be wrong."
        )
    aligned = units_csv.loc[unit_ids]  # exact, ordered to the units table rows

    existing = set(nwbfile.units.colnames)
    added = 0
    for col, description in _ADDED_COLUMN_DESCRIPTIONS.items():
        if col in existing:
            warnings.warn(f"units already has a column named {col!r}; skipping.")
            continue
        series = aligned[col]
        if col == "structure_acronym":
            data = series.fillna("").astype(str).to_numpy()
        elif series.dtype == bool:
            data = series.to_numpy(dtype=bool)
        elif pd.api.types.is_integer_dtype(series.dtype):
            data = series.to_numpy()  # native integer (ids present for every unit)
        else:
            data = series.to_numpy(dtype=float)  # float, NaN where undefined (coords / structure_id)
        nwbfile.units.add_column(name=col, description=description + _PROVENANCE, data=data)
        added += 1
    print(f"  added {added} AllenSDK unit columns to the units table ({len(unit_ids)} units).")
    return added


# --- electrodes: CCF "not registered" sentinel -> NaN ------------------------------------------
# The electrodes table x/y/z axes are the Allen CCFv3 anterior-posterior / dorsal-ventral /
# left-right coordinates. The source VBN NWB already carries the correct coordinates on registered
# channels (verified equal to the released channels.csv on every channel across the dataset), so no
# external re-source is needed -- the only fix is mapping the "not registered to CCF" sentinel,
# which the NWB stores as (0, 0, 0) on all three axes, to NaN.
_ELECTRODE_CCF_AXES = ("x", "y", "z")
_ELECTRODE_CCF_AXIS_DESCRIPTIONS = {
    "x": (
        "Anterior-posterior position of this channel in the Allen CCFv3 (microns); NaN if the "
        "channel was not registered to the CCF."
    ),
    "y": (
        "Dorsal-ventral position of this channel in the Allen CCFv3 (microns); NaN if the channel "
        "was not registered to the CCF."
    ),
    "z": (
        "Left-right (medial-lateral) position of this channel in the Allen CCFv3 (microns); NaN if "
        "the channel was not registered to the CCF."
    ),
}


def null_unregistered_electrode_ccf_coordinates(nwbfile) -> int:
    """Map the electrodes table's ``(0, 0, 0)`` "not registered to CCF" sentinel to NaN, in place.

    The source Visual Behavior Neuropixels NWB electrodes table already carries the correct Allen
    CCFv3 coordinates (``x``/``y``/``z`` = anterior-posterior / dorsal-ventral / left-right) on
    registered channels -- verified exactly equal to the released ``channels.csv`` on every channel
    across the dataset -- so nothing needs to be re-sourced. The only issue is that channels which
    were never registered to the CCF are stored as ``(0, 0, 0)`` on all three axes (the CCF origin
    corner, non-physical for brain tissue) rather than NaN. Registration is per session: 7 of the
    153 ecephys sessions are *entirely* unregistered (every channel ``(0, 0, 0)``) and the rest have
    no zero-sentinel channels. This overwrites those all-axes-zero rows with NaN so unregistered
    channels read as "unknown", mirroring the Visual Coding Neuropixels electrode fix (which maps its
    all-axes ``-1000`` sentinel to NaN). A real ``0`` on a single axis is preserved -- only
    all-three-zero is treated as the sentinel. No-op (returns 0) for behavior-only sessions (no
    electrodes table). Returns the number of electrodes nulled.
    """
    if nwbfile.electrodes is None:
        return 0
    missing = [a for a in _ELECTRODE_CCF_AXES if a not in nwbfile.electrodes.colnames]
    if missing:
        raise RuntimeError(f"electrodes table is missing expected column(s): {missing}.")

    coords = {a: np.asarray(nwbfile.electrodes[a].data[:], dtype=float) for a in _ELECTRODE_CCF_AXES}
    unregistered = np.logical_and.reduce([coords[a] == 0 for a in _ELECTRODE_CCF_AXES])
    n_unreg = int(unregistered.sum())

    for axis, data in coords.items():
        data[unregistered] = np.nan
        # WARNING: overwrite the existing VectorData values in place (same workaround as
        # fix_vector_index_dtypes); validation is performed afterwards on the exported Zarr.
        nwbfile.electrodes[axis]._Data__data = data
        nwbfile.electrodes[axis].fields["description"] = _ELECTRODE_CCF_AXIS_DESCRIPTIONS[axis]
    print(
        f"  nulled the (0,0,0) 'not registered to CCF' sentinel on {n_unreg}/{len(coords['x'])} "
        f"electrodes (x/y/z set to NaN)."
    )
    return n_unreg
