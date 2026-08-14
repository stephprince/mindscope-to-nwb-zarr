"""Add the Allen Brain Observatory DLC eye-gaze-mapping product to a Visual Coding ophys NWB.

The Allen "eye gaze mapping" product (DeepLabCut pupil tracking projected onto the stimulus
monitor) lives in a separate S3 prefix, NOT in the v1 experiment NWBs, so the upstream
converter never ingested it. It exists for 837 of the 1518 ophys experiments as
``ophys_eye_gaze_mapping/<experiment_id>_<ophys_session_id>_eyetracking_dlc_to_screen_mapping.h5``
(~20 MB each).

This module downloads that file at conversion time (from the same public
``allen-brain-observatory`` bucket the conversion already uses) and attaches its contents to
``nwbfile.processing['behavior']`` as pynwb behavior containers. The container/series names are
suffixed with ``_dlc_screen_mapping`` so they never collide with the OLDER v1-embedded
``EyeTracking``/``PupilTracking``/``CompassDirection`` that the DANDI file already carries for an
overlapping subset (178 sessions have both) -- both products coexist.

The H5 is a plain pandas/PyTables HDFStore, read here directly with h5py (no AllenSDK). All
series share ``synced_frame_timestamps`` (seconds, session master clock). Each quantity has a
blink-filtered ``new_*`` variant (NaN where a blink/dropped frame was detected, ~43% of frames)
and an unfiltered ``raw_*`` variant; both are stored.
"""
import re
from pathlib import Path

import h5py
import numpy as np
from pynwb.base import TimeSeries
from pynwb.behavior import CompassDirection, EyeTracking, PupilTracking, SpatialSeries

# S3 prefix (within the bucket the conversion already opens) holding the mapping files.
S3_EYE_GAZE_MAPPING_PREFIX = "visual-coding-2p/ophys_eye_gaze_mapping"

# Suffix that distinguishes this DLC product from the v1-embedded eye tracking.
_SUFFIX = "_dlc_screen_mapping"

_REF_FRAME_CM = "(0,0) is the center of the stimulus monitor. Columns are [x_pos_cm, y_pos_cm]."
_REF_FRAME_DEG = "(0,0) is the center of the stimulus monitor. Columns are [x_pos_deg, y_pos_deg]."


def resolve_eye_gaze_mapping_key(bucket, experiment_id: int) -> str | None:
    """Return the S3 key of the eye-gaze-mapping H5 for this experiment, or ``None``.

    quilt3's ``Bucket.ls`` does not match partial file-name prefixes, so we list the whole
    ``ophys_eye_gaze_mapping/`` folder (838 keys) and filter by the experiment id anchored on
    the full expected file name. Returns ``None`` when the experiment has no mapping file
    (expected for 681 of 1518 sessions -- a normal no-op, not an error); raises if more than
    one file matches (must never happen).
    """
    listing = bucket.ls(f"{S3_EYE_GAZE_MAPPING_PREFIX}/")
    if not listing or len(listing) < 2:
        return None
    pattern = re.compile(rf"/{experiment_id}_\d+_eyetracking_dlc_to_screen_mapping\.h5$")
    keys = [f["Key"] for f in listing[1] if pattern.search(f["Key"])]
    if not keys:
        return None
    if len(keys) > 1:
        raise RuntimeError(
            f"Expected 0 or 1 eye-gaze-mapping files for experiment {experiment_id}, "
            f"found {len(keys)}: {keys}"
        )
    return keys[0]


def _screen_xy(group: h5py.Group, x_name: str, y_name: str) -> np.ndarray:
    """Read a 2-column screen-coordinate DataFrame group as an (N, 2) [x, y] array.

    The stored column order is (y, x); we index by the decoded column names (never by
    position) and emit [x, y] to match the NWB SpatialSeries convention. Raises if the
    columns are not exactly the expected pair.
    """
    items = [name.decode() if isinstance(name, bytes) else str(name)
             for name in group["block0_items"][:]]
    if set(items) != {x_name, y_name}:
        raise RuntimeError(f"Unexpected screen-coordinate columns {items}; expected {[x_name, y_name]}.")
    values = group["block0_values"][:]  # (N, 2)
    return np.column_stack([values[:, items.index(x_name)], values[:, items.index(y_name)]])


def _read_gaze_mapping_h5(h5_path: Path) -> dict:
    """Read the eye-gaze-mapping H5 into numpy arrays, validating lengths against N frames."""
    with h5py.File(h5_path, "r") as f:
        timestamps = f["synced_frame_timestamps"]["values"][:]
        n = len(timestamps)
        arrays = {
            "timestamps": timestamps,
            "screen_cm_new": _screen_xy(f["new_screen_coordinates"], "x_pos_cm", "y_pos_cm"),
            "screen_cm_raw": _screen_xy(f["raw_screen_coordinates"], "x_pos_cm", "y_pos_cm"),
            "screen_deg_new": _screen_xy(f["new_screen_coordinates_spherical"], "x_pos_deg", "y_pos_deg"),
            "screen_deg_raw": _screen_xy(f["raw_screen_coordinates_spherical"], "x_pos_deg", "y_pos_deg"),
            "pupil_area_new": f["new_pupil_areas"]["values"][:],
            "pupil_area_raw": f["raw_pupil_areas"]["values"][:],
            "eye_area_new": f["new_eye_areas"]["values"][:],
            "eye_area_raw": f["raw_eye_areas"]["values"][:],
        }
    for name, arr in arrays.items():
        if len(arr) != n:
            raise RuntimeError(
                f"eye-gaze-mapping array {name!r} has length {len(arr)}, expected {n} "
                f"(== number of synced_frame_timestamps)."
            )
    return arrays


def add_eye_gaze_mapping(nwbfile, experiment_id: int, bucket, scratch_dir: Path) -> bool:
    """Attach the DLC eye-gaze-mapping product to ``nwbfile.processing['behavior']``.

    Downloads the mapping H5 for ``experiment_id`` from ``bucket`` (a quilt3 Bucket for
    ``s3://allen-brain-observatory``) into ``scratch_dir``, reads it, and adds:

    - ``EyeTracking_dlc_screen_mapping``     -- SpatialSeries ``pupil_location*`` (gaze on the
      monitor, cm), blink-filtered + raw.
    - ``CompassDirection_dlc_screen_mapping`` -- SpatialSeries ``pupil_location_spherical*``
      (angular gaze, degrees), blink-filtered + raw.
    - ``PupilTracking_dlc_screen_mapping``   -- TimeSeries ``pupil_area*`` / ``eye_area*``
      (pixel-area), blink-filtered + raw.

    Returns ``True`` if data was added, ``False`` if this experiment has no mapping file
    (no-op). Raises if the ``_dlc_screen_mapping`` containers already exist (fail loud).
    """
    key = resolve_eye_gaze_mapping_key(bucket, experiment_id)
    if key is None:
        print(f"No eye-gaze-mapping file for experiment {experiment_id}; skipping eye-gaze step.")
        return False

    local_path = scratch_dir / Path(key).name
    print(f"Downloading eye-gaze-mapping {Path(key).name} ...")
    bucket.fetch(key, local_path.as_posix())
    arrays = _read_gaze_mapping_h5(local_path)
    timestamps = arrays["timestamps"]

    if "behavior" not in nwbfile.processing.keys():
        nwbfile.create_processing_module(name="behavior", description="Processed behavioral data.")
    behavior = nwbfile.processing["behavior"]

    for container_name in (f"EyeTracking{_SUFFIX}", f"CompassDirection{_SUFFIX}", f"PupilTracking{_SUFFIX}"):
        if container_name in behavior.data_interfaces:
            raise RuntimeError(
                f"{container_name!r} already exists in processing['behavior']; refusing to overwrite."
            )

    eye_tracking = EyeTracking(
        name=f"EyeTracking{_SUFFIX}",
        spatial_series=[
            SpatialSeries(
                name=f"pupil_location{_SUFFIX}",
                data=arrays["screen_cm_new"],
                timestamps=timestamps,
                unit="cm",
                reference_frame=_REF_FRAME_CM,
                description=("DeepLabCut-estimated gaze location projected onto the stimulus "
                             "monitor (cm), blink-filtered (NaN during blinks/dropped frames)."),
            ),
            SpatialSeries(
                name=f"pupil_location_raw{_SUFFIX}",
                data=arrays["screen_cm_raw"],
                timestamps=timestamps,
                unit="cm",
                reference_frame=_REF_FRAME_CM,
                description=("DeepLabCut-estimated gaze location projected onto the stimulus "
                             "monitor (cm), unfiltered (raw)."),
            ),
        ],
    )
    compass_direction = CompassDirection(
        name=f"CompassDirection{_SUFFIX}",
        spatial_series=[
            SpatialSeries(
                name=f"pupil_location_spherical{_SUFFIX}",
                data=arrays["screen_deg_new"],
                timestamps=timestamps,
                unit="degrees",
                reference_frame=_REF_FRAME_DEG,
                description=("DeepLabCut-estimated angular gaze direction on the stimulus "
                             "monitor (degrees), blink-filtered."),
            ),
            SpatialSeries(
                name=f"pupil_location_spherical_raw{_SUFFIX}",
                data=arrays["screen_deg_raw"],
                timestamps=timestamps,
                unit="degrees",
                reference_frame=_REF_FRAME_DEG,
                description=("DeepLabCut-estimated angular gaze direction on the stimulus "
                             "monitor (degrees), unfiltered (raw)."),
            ),
        ],
    )
    pupil_tracking = PupilTracking(
        name=f"PupilTracking{_SUFFIX}",
        time_series=[
            TimeSeries(name=f"pupil_area{_SUFFIX}", data=arrays["pupil_area_new"], timestamps=timestamps,
                       unit="pixels", description="DeepLabCut-estimated pupil area (pixel-area), blink-filtered."),
            TimeSeries(name=f"pupil_area_raw{_SUFFIX}", data=arrays["pupil_area_raw"], timestamps=timestamps,
                       unit="pixels", description="DeepLabCut-estimated pupil area (pixel-area), unfiltered (raw)."),
            TimeSeries(name=f"eye_area{_SUFFIX}", data=arrays["eye_area_new"], timestamps=timestamps,
                       unit="pixels", description="DeepLabCut-estimated eye area (pixel-area), blink-filtered."),
            TimeSeries(name=f"eye_area_raw{_SUFFIX}", data=arrays["eye_area_raw"], timestamps=timestamps,
                       unit="pixels", description="DeepLabCut-estimated eye area (pixel-area), unfiltered (raw)."),
        ],
    )

    behavior.add(eye_tracking)
    behavior.add(compass_direction)
    behavior.add(pupil_tracking)
    print(f"  -> added eye-gaze-mapping ({len(timestamps)} frames) to processing['behavior'] "
          f"as *{_SUFFIX} containers.")
    return True
