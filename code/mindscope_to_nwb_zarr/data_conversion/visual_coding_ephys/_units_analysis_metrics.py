"""Add AllenSDK visual-response analysis metrics to the Visual Coding Neuropixels units table.

The Allen Brain Observatory publishes per-unit *analysis metrics* (receptive-field, tuning,
running-modulation, per-stimulus firing rates, lifetime sparseness, image selectivity) that are
returned by AllenSDK (e.g. ``cache.get_unit_analysis_metrics_by_session_type``) but are **not**
present in the source NWB ``units`` table. AllenSDK is incompatible with this repo's environment
(it needs pynwb 2.x), but the metrics are published as plain CSV data products on the same public
S3 bucket, so they can be streamed and attached with no AllenSDK dependency.

This module also backfills, from the published ``channels.csv``:
  * the numeric ``ecephys_structure_id`` on the *electrodes* table (the NWB electrodes table
    already carries the structure *acronym* as ``location`` but not the numeric CCF id);
  * per-*unit* channel-derived columns copied from each unit's peak channel -- CCF coordinates,
    brain structure (``ecephys_structure_id`` + ``ecephys_structure_acronym``) and on-probe
    geometry (``probe_horizontal_position``, ``probe_vertical_position``, ``ecephys_probe_id``).
    The source NWB units table carries only ``peak_channel_id``; AllenSDK's units table adds these
    by merging units to channels on that key (``EcephysSession._build_units_table``), which this
    reproduces, so the archived units table carries the same channel-derived columns AllenSDK
    returns; and
  * a correction to the electrodes table ``z`` column: the source NWBs have a packaging error in
    which ``z`` duplicates ``y`` (both hold the dorsal-ventral coordinate) instead of the
    left-right (medial-lateral) coordinate, so ``z`` is overwritten with the true
    ``left_right_ccf_coordinate`` from ``channels.csv``.

The ``-1000`` "not registered to CCF" sentinel used by ``channels.csv`` is mapped to NaN wherever
these coordinates are attached (units and the corrected electrode ``z``), matching the NWB's NaN
for registered-but-out-of-brain channels.

Join keys (validated on real sessions): the analysis-metrics CSV is keyed by ``ecephys_unit_id``
which equals the NWB ``units.id``; ``channels.csv`` is keyed by channel id which equals the NWB
``electrodes.id`` and the units' ``peak_channel_id``. The metrics files are dataset-wide (all units
of a session type), so they are reindexed to the session's unit ids; units with no metrics row (a
small per-session fraction, including some noise units) receive NaN.
"""
import warnings
from functools import lru_cache

import numpy as np
import pandas as pd

# Public HTTPS front of s3://allen-brain-observatory/visual-coding-neuropixels/ecephys-cache
S3_CACHE_HTTPS = (
    "https://allen-brain-observatory.s3.amazonaws.com/visual-coding-neuropixels/ecephys-cache"
)

# One analysis-metrics file per session type; column sets differ (functional_connectivity adds
# dot-motion "_dm" metrics and omits the static-gratings/natural-scene metrics).
_METRICS_FILE = {
    "brain_observatory_1.1": "brain_observatory_1.1_analysis_metrics.csv",
    "functional_connectivity": "functional_connectivity_analysis_metrics.csv",
}

_UNIT_ID_KEY = "ecephys_unit_id"

# --- metric-description composition ------------------------------------------------------------
# Metric column names follow "<base>[_multi]_<stimulus>" (e.g. pref_ori_dg, pref_sf_multi_sg,
# firing_rate_ns). Descriptions are composed from a base-metric phrase and a stimulus phrase so we
# cover the full BO1.1 (57) and functional_connectivity (45) column sets without hand-listing each.
_STIMULUS_PHRASE = {
    "dg": "drifting gratings",
    "sg": "static gratings",
    "ns": "natural scenes",
    "fl": "full-field flashes",
    "rf": "receptive-field (gabor) mapping",
    "dm": "dot motion",
}

_BASE_METRIC_PHRASE = {
    "c50": "Contrast at half-maximum response (C50) from the contrast-response function",
    "area": "Receptive-field area",
    "width": "Receptive-field width",
    "height": "Receptive-field height",
    "azimuth": "Receptive-field center azimuth",
    "elevation": "Receptive-field center elevation",
    "p_value": "Significance (p-value) of the receptive-field map",
    "on_screen": "Degree to which the receptive field falls on the stimulus screen",
    "fano": "Fano factor (spike-count variance divided by mean)",
    "f1_f0": "Ratio of the first harmonic to the mean (F1/F0) response",
    "g_dsi": "Global direction-selectivity index",
    "g_osi": "Global orientation-selectivity index",
    "mod_idx": "Modulation index",
    "pref_sf": "Preferred spatial frequency",
    "pref_tf": "Preferred temporal frequency",
    "pref_ori": "Preferred orientation",
    "pref_dir": "Preferred direction",
    "pref_speed": "Preferred speed",
    "pref_phase": "Preferred phase",
    "pref_image": "Preferred image index",
    "pref_images": "Preferred image index",
    "run_mod": "Running modulation (response change between running and stationary)",
    "run_pval": "Significance (p-value) of the running modulation",
    "firing_rate": "Mean firing rate",
    "on_off_ratio": "On/off response ratio",
    "time_to_peak": "Time to peak response",
    "sustained_idx": "Sustained-response index",
    "image_selectivity": "Image selectivity",
    "lifetime_sparseness": "Lifetime sparseness",
}

_PROVENANCE = (
    " Source: Allen Brain Observatory Neuropixels unit analysis metrics "
    "(get_unit_analysis_metrics_by_session_type); not present in the source NWB."
)


def _describe_metric(col: str) -> str:
    """Compose a human-readable description for an analysis-metric column name."""
    tokens = col.split("_")
    stim = tokens[-1]
    multi = len(tokens) >= 2 and tokens[-2] == "multi"
    base = "_".join(tokens[: -(2 if multi else 1)])
    base_phrase = _BASE_METRIC_PHRASE.get(base)
    stim_phrase = _STIMULUS_PHRASE.get(stim)
    if base_phrase is None or stim_phrase is None:
        return f"Visual-response analysis metric '{col}'." + _PROVENANCE
    desc = f"{base_phrase} during {stim_phrase}."
    if multi:
        desc += " Estimated from the combined multi-stimulus analysis."
    return desc + _PROVENANCE


# --- session-type lookup -----------------------------------------------------------------------
def resolve_session_type(session_id: int, s3_cache: str = S3_CACHE_HTTPS) -> str:
    """Return the session type ('brain_observatory_1.1' or 'functional_connectivity').

    Read from the published ``sessions.csv`` (a small table), keyed by session id.
    """
    sessions = pd.read_csv(f"{s3_cache}/sessions.csv").set_index("id")
    session_type = str(sessions.loc[int(session_id), "session_type"])
    if session_type not in _METRICS_FILE:
        raise ValueError(
            f"Session {session_id} has session_type {session_type!r}; expected one of "
            f"{list(_METRICS_FILE)}."
        )
    return session_type


# --- units: analysis metrics -------------------------------------------------------------------
def add_unit_analysis_metrics(nwbfile, session_type: str, s3_cache: str = S3_CACHE_HTTPS) -> int:
    """Attach the visual-response analysis metrics for ``session_type`` to ``nwbfile.units``.

    Streams the session type's metrics CSV, reindexes it to ``nwbfile.units.id`` (so every unit
    gets a row, NaN where the unit has no metrics), and adds one float column per metric. Returns
    the number of units that matched a metrics row. Raises if the session type is unknown or if
    no unit matches (which would indicate a join-key regression).
    """
    if session_type not in _METRICS_FILE:
        raise ValueError(f"Unknown session_type {session_type!r}; expected {list(_METRICS_FILE)}.")

    url = f"{s3_cache}/{_METRICS_FILE[session_type]}"
    print(f"Loading unit analysis metrics from {url} ...")
    metrics = pd.read_csv(url).set_index(_UNIT_ID_KEY)
    if metrics.index.has_duplicates:
        raise ValueError(f"Duplicate {_UNIT_ID_KEY} values in {url}.")

    unit_ids = np.asarray(nwbfile.units.id[:])
    aligned = metrics.reindex(unit_ids)
    n_matched = int(aligned.notna().any(axis=1).sum())
    if n_matched == 0:
        raise RuntimeError(
            f"No units matched the analysis metrics for session_type {session_type!r} "
            f"(checked {len(unit_ids)} unit ids against {url}); join key may be wrong."
        )
    print(
        f"  matched {n_matched}/{len(unit_ids)} units to analysis metrics "
        f"({len(unit_ids) - n_matched} units get NaN metric values)."
    )

    existing = set(nwbfile.units.colnames)
    added = 0
    for col in metrics.columns:
        if col in existing:
            warnings.warn(f"units already has a column named {col!r}; skipping analysis metric.")
            continue
        # Coerce to float so NaN-fill is valid and the Zarr column has a uniform numeric dtype
        # (booleans like on_screen_rf become 1.0/0.0/NaN; integer image indices become floats).
        data = pd.to_numeric(aligned[col], errors="coerce").to_numpy(dtype=float)
        nwbfile.units.add_column(name=col, description=_describe_metric(col), data=data)
        added += 1
    print(f"  added {added} analysis-metric columns to the units table.")
    return n_matched


# --- channels.csv (shared by the electrode + unit backfills) -----------------------------------
_CHANNELS_PROVENANCE = (
    " Source: Allen Brain Observatory Neuropixels channels.csv "
    "(joined to the unit's peak_channel_id)."
)


@lru_cache(maxsize=2)
def _load_channels(s3_cache: str = S3_CACHE_HTTPS) -> pd.DataFrame:
    """Load the published ``channels.csv`` (dataset-wide), indexed by channel id.

    Cached so the electrode-structure and unit-CCF backfills fetch this ~6 MB CSV only once per
    conversion. Callers only reindex (never mutate) the returned frame, so sharing is safe.
    """
    url = f"{s3_cache}/channels.csv"
    print(f"Loading channels table from {url} ...")
    return pd.read_csv(url).set_index("id")


# --- electrodes: numeric structure id ----------------------------------------------------------
def add_electrode_structure_ids(nwbfile, s3_cache: str = S3_CACHE_HTTPS) -> int:
    """Add numeric ``ecephys_structure_id`` to the electrodes table from ``channels.csv``.

    The NWB electrodes table carries the structure *acronym* (``location``) but not the numeric
    Allen CCFv3 structure id. Reindexes ``channels.csv`` (keyed by channel id == electrode id) to
    the electrodes, adding NaN where a channel is absent or has no assigned structure. Returns the
    number of electrodes with a non-null structure id.
    """
    if nwbfile.electrodes is None:
        return 0
    if "ecephys_structure_id" in nwbfile.electrodes.colnames:
        warnings.warn("electrodes already has 'ecephys_structure_id'; skipping.")
        return 0

    channels = _load_channels(s3_cache)
    if "ecephys_structure_id" not in channels.columns:
        raise RuntimeError(f"'ecephys_structure_id' column missing from {s3_cache}/channels.csv.")

    electrode_ids = np.asarray(nwbfile.electrodes.id[:])
    structure_id = pd.to_numeric(
        channels.reindex(electrode_ids)["ecephys_structure_id"], errors="coerce"
    ).to_numpy(dtype=float)
    n_known = int(np.isfinite(structure_id).sum())
    nwbfile.electrodes.add_column(
        name="ecephys_structure_id",
        description=(
            "Allen CCFv3 structure ID of the brain region containing this channel (numeric "
            "counterpart of the 'location' acronym); NaN if the channel is out of brain or "
            "unassigned. Source: Allen Brain Observatory Neuropixels channels.csv."
        ),
        data=structure_id,
    )
    print(f"  set ecephys_structure_id for {n_known}/{len(electrode_ids)} electrodes.")
    return n_known


# --- units: CCF coordinates + brain structure --------------------------------------------------
# Numeric per-unit columns copied from the unit's peak channel, with descriptions. This mirrors the
# AllenSDK units table, which adds these by merging units to channels on peak_channel_id
# (EcephysSession._build_units_table). Coordinates and the numeric structure id are floats (NaN
# where the peak channel is out of brain / unregistered); the structure acronym is handled
# separately as a string column.
_UNIT_CCF_NUMERIC_COLUMNS = {
    "anterior_posterior_ccf_coordinate": (
        "Anterior-posterior position of this unit's peak channel in the Allen CCFv3 (microns); "
        "NaN if the channel is out of brain or unregistered."
    ),
    "dorsal_ventral_ccf_coordinate": (
        "Dorsal-ventral position of this unit's peak channel in the Allen CCFv3 (microns); "
        "NaN if the channel is out of brain or unregistered."
    ),
    "left_right_ccf_coordinate": (
        "Left-right position of this unit's peak channel in the Allen CCFv3 (microns); "
        "NaN if the channel is out of brain or unregistered."
    ),
    "ecephys_structure_id": (
        "Allen CCFv3 structure ID of the brain region containing this unit's peak channel "
        "(numeric counterpart of the 'ecephys_structure_acronym'); NaN if the channel is out of "
        "brain or unassigned."
    ),
}
_UNIT_STRUCTURE_ACRONYM_COLUMN = "ecephys_structure_acronym"

# On-probe geometry columns copied from the unit's peak channel (independent of CCF registration:
# a channel always has a probe position even when it is not registered to the CCF). These are the
# channel-derived columns the AllenSDK units table exposes beyond the CCF coordinates/structure.
# Stored as float so an unmatched peak channel (absent from channels.csv) reads as NaN.
_UNIT_CHANNEL_GEOMETRY_COLUMNS = {
    "probe_horizontal_position": (
        "Horizontal (across-probe) position (microns) of this unit's peak channel."
    ),
    "probe_vertical_position": (
        "Distance (microns) from the probe tip to this unit's peak channel along the probe."
    ),
    "ecephys_probe_id": (
        "Identifier of the Neuropixels probe that recorded this unit (the probe of its peak "
        "channel)."
    ),
}

# channels.csv marks a channel that was never registered to the CCF with -1000 on all three axes
# (a non-physical sentinel, not NaN). AllenSDK's two products disagree on this and it does no
# conversion of its own: the warehouse/CSV path (EcephysProjectCache.get_units/get_channels) exposes
# -1000, while the NWB path (EcephysSession.units, via Channels.from_nwb reading the electrodes'
# x/y/z) exposes NaN -- verified by streaming the source NWBs (session 760693773 unregistered -> NaN,
# session 719161530 registered -> real coords matching the CSV). We map the sentinel to NaN so the
# per-unit coordinates match the NWB path *and* the electrodes table in this same Zarr (which
# inherits the NWB's NaN), rather than storing -1000, which would contradict the in-file electrodes
# and let a downstream consumer plot -1000 as a real location. The numeric structure id / acronym are
# independent of this sentinel (a channel can be -1000 yet still carry a coarse structure like
# 'grey') and come through as NaN / "" on their own.
_CCF_COORD_COLUMNS = (
    "anterior_posterior_ccf_coordinate",
    "dorsal_ventral_ccf_coordinate",
    "left_right_ccf_coordinate",
)
_CCF_MISSING_SENTINEL = -1000


def add_unit_channel_columns(nwbfile, s3_cache: str = S3_CACHE_HTTPS) -> int:
    """Add per-unit peak-channel columns to ``nwbfile.units`` from ``channels.csv``.

    The source NWB units table carries only ``peak_channel_id``; the CCF coordinates, brain
    structure and on-probe geometry live on the channels/electrodes. This reproduces AllenSDK's
    units table (``EcephysSession._build_units_table`` merges units to channels on
    ``peak_channel_id``) by copying, from the published ``channels.csv``, each unit's peak-channel
    CCF coordinates, numeric structure id + structure acronym, and probe geometry
    (``probe_horizontal_position``, ``probe_vertical_position``, ``ecephys_probe_id``) so the units
    table carries the same channel-derived columns AllenSDK returns. Numeric columns are floats (NaN
    where the peak channel is out of brain / unregistered, or where the channel is absent from
    channels.csv); the acronym is a string ("" where unassigned). Returns the number of units that
    matched a channel row. Raises if the join key is missing or matches nothing (which would
    indicate a regression).
    """
    if nwbfile.units is None:
        return 0
    if "peak_channel_id" not in nwbfile.units.colnames:
        raise RuntimeError(
            "units table has no 'peak_channel_id' column; cannot join channel columns from "
            "channels.csv."
        )

    channels = _load_channels(s3_cache)
    expected = (*_UNIT_CCF_NUMERIC_COLUMNS, _UNIT_STRUCTURE_ACRONYM_COLUMN,
                *_UNIT_CHANNEL_GEOMETRY_COLUMNS)
    missing = [c for c in expected if c not in channels.columns]
    if missing:
        raise RuntimeError(f"channels.csv is missing expected column(s): {missing}.")

    # peak_channel_id is the channels.csv index (channel id); keep its integer dtype so reindex
    # matches by value. Channels for every probe in the session are present in the file, so an
    # unmatched id yields an all-NaN row (unit filtered out of brain / not registered).
    peak_channel_id = np.asarray(nwbfile.units["peak_channel_id"].data[:])
    aligned = channels.reindex(peak_channel_id)
    n_matched = int(aligned.notna().any(axis=1).sum())
    if n_matched == 0:
        raise RuntimeError(
            f"No units matched a channel row when joining on peak_channel_id "
            f"(checked {len(peak_channel_id)} units against {s3_cache}/channels.csv); "
            f"join key may be wrong."
        )
    print(
        f"  matched {n_matched}/{len(peak_channel_id)} units to their peak channel "
        f"({len(peak_channel_id) - n_matched} units get NaN CCF positions)."
    )

    # Coordinates as float, with the all-axes -1000 "not registered to CCF" sentinel mapped to NaN.
    coords = aligned[list(_CCF_COORD_COLUMNS)].apply(pd.to_numeric, errors="coerce")
    unregistered = (coords == _CCF_MISSING_SENTINEL).all(axis=1)
    coords[unregistered] = np.nan
    numeric = coords.assign(
        ecephys_structure_id=pd.to_numeric(aligned["ecephys_structure_id"], errors="coerce")
    )

    existing = set(nwbfile.units.colnames)
    added = 0
    for col, description in _UNIT_CCF_NUMERIC_COLUMNS.items():
        if col in existing:
            warnings.warn(f"units already has a column named {col!r}; skipping unit CCF position.")
            continue
        data = numeric[col].to_numpy(dtype=float)
        nwbfile.units.add_column(name=col, description=description + _CHANNELS_PROVENANCE, data=data)
        added += 1

    if _UNIT_STRUCTURE_ACRONYM_COLUMN in existing:
        warnings.warn(
            f"units already has a column named {_UNIT_STRUCTURE_ACRONYM_COLUMN!r}; skipping."
        )
    else:
        acronym = aligned[_UNIT_STRUCTURE_ACRONYM_COLUMN].fillna("").astype(str).to_numpy()
        nwbfile.units.add_column(
            name=_UNIT_STRUCTURE_ACRONYM_COLUMN,
            description=(
                "Acronym of the Allen CCFv3 brain structure containing this unit's peak channel "
                "(the unit's recorded location); empty if the channel is out of brain or "
                "unassigned." + _CHANNELS_PROVENANCE
            ),
            data=acronym,
        )
        added += 1

    # On-probe geometry of the peak channel (probe id + position). Not subject to the CCF
    # sentinel: a channel keeps its probe position even when unregistered to the CCF, so these are
    # read straight from the aligned channel row (NaN only where the channel is absent entirely).
    for col, description in _UNIT_CHANNEL_GEOMETRY_COLUMNS.items():
        if col in existing:
            warnings.warn(f"units already has a column named {col!r}; skipping unit probe geometry.")
            continue
        data = pd.to_numeric(aligned[col], errors="coerce").to_numpy(dtype=float)
        nwbfile.units.add_column(name=col, description=description + _CHANNELS_PROVENANCE, data=data)
        added += 1
    print(f"  added {added} unit channel-derived columns to the units table.")
    return n_matched


def replace_electrode_z_with_ccf_left_right(nwbfile, s3_cache: str = S3_CACHE_HTTPS) -> int:
    """Overwrite the electrodes table ``z`` column with the CCF left-right coordinate.

    The source Visual Coding Neuropixels NWBs have a packaging error in which the electrodes table
    ``z`` column duplicates ``y`` (both hold the dorsal-ventral CCF coordinate) instead of the
    left-right (medial-lateral) coordinate. This overwrites ``z`` with the true
    ``left_right_ccf_coordinate`` from the published ``channels.csv`` (joined on electrode id ==
    channel id), mapping the ``-1000`` "not registered to CCF" sentinel to NaN so unregistered
    channels read as NaN -- matching the NWB's NaN for ``x``/``y`` and the per-unit CCF columns in
    this same file. Electrodes absent from channels.csv also receive NaN. Returns the number of
    electrodes assigned a finite left-right coordinate.
    """
    if nwbfile.electrodes is None:
        return 0
    channels = _load_channels(s3_cache)
    if "left_right_ccf_coordinate" not in channels.columns:
        raise RuntimeError(
            f"'left_right_ccf_coordinate' column missing from {s3_cache}/channels.csv."
        )

    electrode_ids = np.asarray(nwbfile.electrodes.id[:])
    left_right = pd.to_numeric(
        channels.reindex(electrode_ids)["left_right_ccf_coordinate"], errors="coerce"
    ).to_numpy(dtype=float)
    left_right[left_right == _CCF_MISSING_SENTINEL] = np.nan
    n_known = int(np.isfinite(left_right).sum())

    # WARNING: overwrite the existing VectorData values in place (same workaround as
    # fix_vector_index_dtypes); validation is performed afterwards on the exported Zarr.
    nwbfile.electrodes["z"]._Data__data = left_right
    nwbfile.electrodes["z"].fields["description"] = (
        "Left-right (medial-lateral) position of this channel in the Allen CCFv3 (microns); NaN if "
        "the channel is out of brain or unregistered. Overwritten from the Allen Brain Observatory "
        "Neuropixels channels.csv to correct a packaging error in the source NWB, in which this "
        "column duplicated the dorsal-ventral coordinate ('y')."
    )
    print(
        f"  replaced electrodes 'z' with left_right_ccf_coordinate for "
        f"{n_known}/{len(electrode_ids)} electrodes (rest NaN)."
    )
    return n_known
