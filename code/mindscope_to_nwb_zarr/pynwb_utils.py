from pathlib import Path

import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from pynwb import NWBFile
from pynwb.base import TimeSeries
from pynwb.ecephys import ElectricalSeries
from pynwb.epoch import TimeIntervals
from hdmf.common import DynamicTable

from aind_data_schema_models.modalities import Modality

def get_latest_time(nwbfile: NWBFile) -> float | None:
    """Calculate latest time from NWB file by finding the latest timestamp across all TimeSeries"""
    max_time = None

    for obj in nwbfile.all_children():
         # get last timestamp across all TimeSeries
        if isinstance(obj, TimeSeries):
            if obj.timestamps is not None and len(obj.timestamps) > 0:
                last_time = obj.timestamps[-1]
            elif obj.starting_time is not None and obj.rate is not None:
                last_time = obj.starting_time + (obj.data.shape[0] / obj.rate)

        # Handle DynamicTable objects with time columns
        elif isinstance(obj, DynamicTable):
            if "stop_time" in obj.colnames and len(obj["stop_time"]):
                last_time = float(obj["stop_time"][-1])
            elif "spike_times" in obj.colnames and len(obj["spike_times"]):
                last_time = max(np.asarray(obj["spike_times"].target.data[:]))
            else:
                continue
        else:
            continue

        if max_time is None or last_time > max_time:
            max_time = last_time
            
    return max_time

def get_data_stream_end_time(nwbfile: NWBFile, session_start_time: datetime = None) -> datetime | None:
    """Calculate acquisition end time from NWB file by finding the latest timestamp across all TimeSeries.

    Timestamps in the file are seconds relative to the session start. By default the
    absolute end time is anchored to ``nwbfile.session_start_time``; pass
    ``session_start_time`` to re-anchor to a corrected start (e.g. when the file's
    session_start_time is a packaging date rather than the real acquisition time).
    """
    latest_time = get_latest_time(nwbfile)
    if session_start_time is None:
        session_start_time = nwbfile.session_start_time

    # Calculate end time
    if latest_time is not None:
        end_time = session_start_time + timedelta(seconds=float(latest_time))
    else:
        end_time = None

    return end_time

def get_data_stream_start_time(nwbfile: NWBFile, session_start_time: datetime = None) -> datetime | None:
    earliest_time = get_earliest_time(nwbfile)
    if session_start_time is None:
        session_start_time = nwbfile.session_start_time

    # Calculate end time
    if earliest_time is not None:
        start_time = session_start_time + timedelta(seconds=float(earliest_time))
    else:
        start_time = None

    return start_time

def get_modalities(nwbfile: NWBFile) -> list[Modality]:
    modalities = set()
    # determine if ecephys modality present
    if nwbfile.units and len(nwbfile.units) > 0:
        modalities.add(Modality.ECEPHYS)

    electrical_series_types = [c for c in nwbfile.all_children() if isinstance(c, ElectricalSeries)]
    if len(electrical_series_types) > 0:
        modalities.add(Modality.ECEPHYS)

    # determine if behavior modality present (is this the best way to check?)
    if nwbfile.trials and len(nwbfile.trials) > 0:
        modalities.add(Modality.BEHAVIOR)

    if nwbfile.imaging_planes and len(nwbfile.imaging_planes) > 0:
        modalities.add(Modality.POPHYS)

    # Behavior videos (eye + body cameras) were recorded for every Allen Brain
    # Observatory experiment, even when that camera data is not packaged in the NWB.
    modalities.add(Modality.BEHAVIOR_VIDEOS)

    return list(modalities)


def get_earliest_time(nwbfile: NWBFile) -> datetime | None:
    """Calculate data stream start time from NWB file by finding the earliest timestamp across all TimeSeries"""
    earliest_time = None

    # get last timestamp across all TimeSeries
    for obj in nwbfile.all_children():
        if isinstance(obj, TimeSeries):
            if obj.timestamps is not None and len(obj.timestamps) > 0:
                start_time = obj.timestamps[0]
            elif obj.starting_time is not None and obj.rate is not None:
                start_time = obj.starting_time
            
        elif isinstance(obj, DynamicTable):
            if "start_time" in obj.colnames and len(obj["start_time"]):
                start_time = float(obj["start_time"][0])
            elif "spike_times" in obj.colnames and len(obj["spike_times"]):
                start_time = min(np.asarray(obj["spike_times"].target.data[:]) )
            else:
                continue
        else:
            continue

        if earliest_time is None or start_time < earliest_time:
            earliest_time = start_time

    return earliest_time


# ---------------------------------------------------------------------------
# Visual Coding ophys: reconstruct the missing `epochs` table for 34 sessions.
#
# The Allen Brain Observatory v1 NWBs store an empty `epoch` group; the real
# stimulus-epoch table is *computed* by the AllenSDK
# (BrainObservatoryNwbDataSet.get_stimulus_epoch_table -> get_epoch_mask_list).
# The upstream converter (catalystneuro/visual-coding-to-nwb-v2) precomputed those
# and skipped any session where the AllenSDK raised EpochSeparationException
# ("more than 2 epochs cut"). That happens when a stimulus splits into >3 blocks
# because a single sub-second dropped-frame gap barely exceeds the AllenSDK's tight
# frame threshold. 34 of 1518 sessions were skipped and have no `epochs`/`intervals`.
#
# We reconstruct the same block structure directly from `nwb.stimulus` in the time
# domain: each stimulus is presented in <=3 contiguous blocks separated by many
# minutes, while intra-block presentations are <~2 s apart, so splitting on a large
# time gap recovers the true blocks and ignores the dropped-frame artifacts. This
# needs no AllenSDK. Validated to reproduce the stored epochs exactly on good sessions.
# ---------------------------------------------------------------------------

# Canonical location of the AllenSDK-derived static_gratings cache (written by
# scripts/extract_vc_ophys_static_gratings.py). This file lives at
# code/mindscope_to_nwb_zarr/pynwb_utils.py, so parents[2] is the repo root, whose
# `data/` is the Code Ocean data-asset mount.
VISUAL_CODING_STATIC_GRATINGS_DIR = (
    Path(__file__).resolve().parents[2] / "data" / "visual-coding-ophys-static-gratings"
)

# Split presentations into separate blocks when the gap between one presentation's end
# and the next one's start exceeds this many seconds. The real block-separating gaps are
# hundreds of seconds; the dropped-frame artifacts that tripped the AllenSDK are < ~1.5 s,
# so any threshold between ~2 s and ~900 s is safe. 10 s leaves a large margin either way.
_EPOCH_GAP_SECONDS = 10.0
# Brain Observatory presents each stimulus in at most 3 non-contiguous blocks per session
# (de Vries et al., 2020). More than this means the gap threshold is wrong or the timing is
# anomalous -- fail loudly rather than emit a bad table.
_MAX_BLOCKS_PER_STIMULUS = 3
# Every real block spans minutes; anything shorter is a stray-presentation artifact.
_MIN_BLOCK_DURATION_SECONDS = 5.0
# Numerical tolerance for the non-overlap and stored-epochs consistency checks.
_BLOCK_TIME_TOLERANCE_SECONDS = 1.0


def _stimulus_presentation_times(base_name, stimulus_obj, experiment_id, static_gratings_dir):
    """Return (onsets, offsets) arrays in seconds for a stimulus object, or (None, None) to skip.

    static_gratings is truncated to 3 rows in the DANDI source (upstream bug #49); when a
    corrected cache CSV is available it is used instead so the static_gratings blocks are
    complete. TimeIntervals stimuli (gratings, spontaneous, locally-sparse-noise) use
    start/stop; template-indexed IndexSeries/TimeSeries stimuli (natural scenes/movies) use
    their per-presentation timestamps as point events (offset == onset).
    """
    if base_name == "static_gratings" and static_gratings_dir is not None and experiment_id is not None:
        csv = Path(static_gratings_dir) / f"{experiment_id}.csv"
        if csv.exists():
            df = pd.read_csv(csv)
            return df["start_time"].to_numpy(float), df["stop_time"].to_numpy(float)
    if isinstance(stimulus_obj, TimeIntervals):
        return (np.asarray(stimulus_obj.start_time[:], dtype=float),
                np.asarray(stimulus_obj.stop_time[:], dtype=float))
    timestamps = getattr(stimulus_obj, "timestamps", None)
    if timestamps is not None:
        ts = np.asarray(timestamps[:], dtype=float)
        return ts, ts
    return None, None


def _split_into_blocks(onsets, offsets, gap_seconds):
    """Split sorted presentations into (start, stop, n_presentations) blocks on large gaps."""
    order = np.argsort(onsets)
    onsets, offsets = onsets[order], offsets[order]
    cut_inds = np.where(onsets[1:] - offsets[:-1] > gap_seconds)[0] + 1
    bounds = [0, *cut_inds.tolist(), len(onsets)]
    return [
        (float(onsets[bounds[i]]), float(offsets[bounds[i + 1] - 1]), int(bounds[i + 1] - bounds[i]))
        for i in range(len(bounds) - 1)
    ]


def reconstruct_stimulus_epochs_table(
    nwbfile: NWBFile,
    experiment_id: int | None = None,
    static_gratings_dir=VISUAL_CODING_STATIC_GRATINGS_DIR,
    gap_seconds: float = _EPOCH_GAP_SECONDS,
) -> TimeIntervals:
    """Reconstruct the Visual Coding ophys ``epochs`` table from ``nwb.stimulus``.

    For the 34 sessions whose DANDI file has no ``epochs`` table (see module notes above),
    this rebuilds one row per contiguous stimulus block by splitting each stimulus's
    presentations on inter-presentation time gaps > ``gap_seconds``. Returns a
    ``TimeIntervals`` named ``"epochs"`` with a ``stimulus_type`` column, matching the schema
    the upstream converter used, so the rest of the pipeline consumes it unchanged.

    Sanity checks (raise ``ValueError`` -- fail loud, never emit a bad epochs table):
    each stimulus yields 1-3 blocks; every block is >= a few seconds and non-overlapping; and
    if the file already has an ``epochs`` table, the reconstruction must reproduce it.

    Args:
        nwbfile: NWB file whose ``stimulus`` group holds the per-stimulus presentation objects.
        experiment_id: ophys experiment id, used to locate the static_gratings cache CSV.
        static_gratings_dir: directory of cached corrected static_gratings tables.
        gap_seconds: block-splitting gap threshold in seconds.

    Returns:
        A ``TimeIntervals`` (name ``"epochs"``) with columns ``start_time``, ``stop_time``,
        ``stimulus_type``.
    """
    blocks: list[tuple[str, float, float, int]] = []
    for name, stimulus_obj in nwbfile.stimulus.items():
        base = name[:-len("_stimulus")] if name.endswith("_stimulus") else name
        onsets, offsets = _stimulus_presentation_times(base, stimulus_obj, experiment_id, static_gratings_dir)
        if onsets is None or len(onsets) == 0:
            continue
        stim_blocks = _split_into_blocks(onsets, offsets, gap_seconds)
        if not (1 <= len(stim_blocks) <= _MAX_BLOCKS_PER_STIMULUS):
            raise ValueError(
                f"Stimulus {base!r} reconstructed into {len(stim_blocks)} blocks (expected "
                f"1-{_MAX_BLOCKS_PER_STIMULUS}); gap_seconds={gap_seconds} is likely wrong or the "
                f"session timing is anomalous. Refusing to write a bad epochs table."
            )
        for start, stop, n_pres in stim_blocks:
            blocks.append((base, start, stop, n_pres))

    if not blocks:
        raise ValueError("No stimulus presentations found in nwb.stimulus; cannot reconstruct epochs.")

    blocks.sort(key=lambda b: b[1])

    prev_stop = -np.inf
    for base, start, stop, _ in blocks:
        if stop <= start:
            raise ValueError(f"Reconstructed a non-positive-duration block for {base!r}: [{start}, {stop}].")
        if (stop - start) < _MIN_BLOCK_DURATION_SECONDS:
            raise ValueError(
                f"Reconstructed an implausibly short block ({stop - start:.2f}s) for {base!r} at "
                f"{start:.2f}s (expected >= {_MIN_BLOCK_DURATION_SECONDS}s). Refusing to write a bad table."
            )
        if start < prev_stop - _BLOCK_TIME_TOLERANCE_SECONDS:
            raise ValueError(
                f"Reconstructed overlapping epochs: {base!r} block starts at {start:.2f}s, before the "
                f"previous block ends at {prev_stop:.2f}s."
            )
        prev_stop = stop

    # Consistency guard: never diverge from a stored epochs table if one exists.
    if nwbfile.epochs is not None:
        stored = nwbfile.epochs.to_dataframe().sort_values("start_time")
        if len(stored) != len(blocks):
            raise ValueError(
                f"Reconstruction produced {len(blocks)} epochs but the file's stored epochs table has "
                f"{len(stored)}; refusing to diverge from the stored epochs."
            )
        for (base, start, _stop, _), (_, row) in zip(blocks, stored.iterrows()):
            if str(row["stimulus_type"]) != base or abs(float(row["start_time"]) - start) > _BLOCK_TIME_TOLERANCE_SECONDS:
                raise ValueError(
                    f"Reconstructed block ({base!r} @ {start:.2f}s) disagrees with the stored epochs "
                    f"({row['stimulus_type']!r} @ {float(row['start_time']):.2f}s)."
                )

    table = TimeIntervals(
        name="epochs",
        description=(
            "Coarse-grain experiment structure: contiguous blocks of each visual stimulus. "
            "Reconstructed from the per-stimulus presentation times in nwb.stimulus by splitting "
            f"on inter-presentation gaps > {gap_seconds:.0f}s, because the source DANDI file lacks "
            "the epochs table (AllenSDK get_stimulus_epoch_table raised EpochSeparationException)."
        ),
    )
    table.add_column(name="stimulus_type", description="Type of visual stimulus presented during the block.")
    for base, start, stop, _ in blocks:
        table.add_interval(start_time=start, stop_time=stop, stimulus_type=base)
    return table
