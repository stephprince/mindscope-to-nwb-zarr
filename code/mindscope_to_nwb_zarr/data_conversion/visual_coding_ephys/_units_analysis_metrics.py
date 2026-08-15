"""Add AllenSDK visual-response analysis metrics to the Visual Coding Neuropixels units table.

The Allen Brain Observatory publishes per-unit *analysis metrics* (receptive-field, tuning,
running-modulation, per-stimulus firing rates, lifetime sparseness, image selectivity) that are
returned by AllenSDK (e.g. ``cache.get_unit_analysis_metrics_by_session_type``) but are **not**
present in the source NWB ``units`` table. AllenSDK is incompatible with this repo's environment
(it needs pynwb 2.x), but the metrics are published as plain CSV data products on the same public
S3 bucket, so they can be streamed and attached with no AllenSDK dependency.

This module also backfills the numeric ``ecephys_structure_id`` on the electrodes table from the
published ``channels.csv`` (the NWB electrodes table already carries the structure *acronym* as
``location`` but not the numeric CCF id).

Join keys (validated on real sessions): the analysis-metrics CSV is keyed by ``ecephys_unit_id``
which equals the NWB ``units.id``; ``channels.csv`` is keyed by channel id which equals the NWB
``electrodes.id``. The metrics files are dataset-wide (all units of a session type), so they are
reindexed to the session's unit ids; units with no metrics row (a small per-session fraction,
including some noise units) receive NaN.
"""
import warnings

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

    url = f"{s3_cache}/channels.csv"
    print(f"Loading channel structure ids from {url} ...")
    channels = pd.read_csv(url).set_index("id")
    if "ecephys_structure_id" not in channels.columns:
        raise RuntimeError(f"'ecephys_structure_id' column missing from {url}.")

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
