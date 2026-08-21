"""Generates acquisition metadata for visual behavior ophys behavior-only sessions"""

import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pynwb import NWBFile

from aind_data_schema.components.identifiers import Code
from aind_data_schema.core.acquisition import (
    Acquisition,
    StimulusEpoch,
    DataStream,
    AcquisitionSubjectDetails,
)
from aind_data_schema.components.configs import (
    LickSpoutConfig,
    Liquid,
    Valence,
)
from aind_data_schema.components.coordinates import (
    CoordinateSystemLibrary,
)
from aind_data_schema.components.stimulus import VisualStimulation
from aind_data_schema_models.units import VolumeUnit, MassUnit
from aind_data_schema_models.stimulus_modality import StimulusModality
from aind_data_schema_models.modalities import Modality

from mindscope_to_nwb_zarr.pynwb_utils import (
    get_data_stream_start_time,
    get_data_stream_end_time,
    get_modalities
)
from mindscope_to_nwb_zarr.aind_data_schema.utils import (
    get_subject_id,
    get_session_start_time,
    get_instrument_id,
    get_total_reward_volume,
    get_individual_reward_volume,
    get_reward_volume_notes,
    get_ethics_review_id,
    convert_intervals_to_visual_stimulus_epoch,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.instrument import (
    REWARD_SPOUT_NAME,
    RUNNING_DISC_NAME,
    STIMULUS_MONITOR_NAME,
)


# Prior-experience descriptors of the change-detection task, recorded as stimulus parameters
# on the epoch instead of being packed into the ``curriculum_status`` string -- mirroring the
# Visual Behavior Neuropixels pipeline (see ``visual_behavior_ephys/acquisition.py``). Sourced
# from the session table (behavior_session_table for behavior-only sessions,
# ophys_experiment_table for ophys sessions; both carry these columns). ``image_set`` is
# intentionally excluded -- it is already the ``VisualStimulation.stimulus_name`` -- so it is
# not duplicated here.
_TASK_EXPERIENCE_COLUMNS = [
    "experience_level",
    "prior_exposures_to_image_set",
    "prior_exposures_to_omissions",
    "prior_exposures_to_session_type",
]


def _clean_value(value):
    """Coerce a session-table cell to a JSON-friendly scalar: None for missing/NaN, a plain
    Python int for integer-valued numbers (numpy or float), otherwise the value itself.

    Mirrors the identically named helper in the Visual Behavior Neuropixels pipeline.
    """
    if pd.isna(value):
        return None
    if hasattr(value, "item"):  # numpy scalar -> Python scalar
        value = value.item()
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _parse_grating_orientations(control_descriptions: list) -> list[float]:
    """Parse grating orientations (degrees) from a grating template's ``control_description``.

    The Visual Behavior ``grating`` stimulus template names each orientation
    ``gratings_<deg>`` (e.g. ``"gratings_0.0"``, ``"gratings_90.0"``). Returns the parsed
    degrees in order. Raises ``ValueError`` on any entry that does not match, so an
    unexpected format fails loudly rather than silently mislabeling orientations.
    """
    orientations = []
    for desc in control_descriptions:
        match = re.fullmatch(r"gratings?_(-?\d+(?:\.\d+)?)", str(desc))
        if not match:
            raise ValueError(
                f"Unexpected grating control_description entry {desc!r}; expected 'gratings_<degrees>'."
            )
        orientations.append(float(match.group(1)))
    return orientations


def get_visual_stimulation(nwbfile: NWBFile, session_info: pd.Series) -> VisualStimulation:
    """Extract visual stimulation information from NWB file"""
    task_parameters = nwbfile.lab_meta_data["task_parameters"]
    stimulus_parameters = {
        "distribution": task_parameters.stimulus_distribution,
        "duration_sec": task_parameters.stimulus_duration_sec,
        "blank_duration_sec": task_parameters.blank_duration_sec,
        "n_stimulus_frames": task_parameters.n_stimulus_frames,
        "response_window_sec": task_parameters.response_window_sec,
        "omitted_flash_fraction": task_parameters.omitted_flash_fraction,
        # NOTE: grating spatial frequencies are not recorded in the data or whitepaper.
    }

    # Record the individual template image names from whichever stimulus template the
    # session used ("grating" on gratings-stage sessions, a natural-images set otherwise);
    # grating orientations are only meaningful for the grating template, so they are added
    # only then -- image-set sessions get no (previously hard-coded, inapplicable) orientations.
    stimulus_template_name: list = []
    for name, template in nwbfile.stimulus_template.items():
        control_descriptions = getattr(template, "control_description", None)
        if control_descriptions is None:
            continue
        control_descriptions = control_descriptions[:].tolist()
        stimulus_template_name.extend(control_descriptions)
        if name == "grating":
            stimulus_parameters["grating_orientations"] = _parse_grating_orientations(control_descriptions)
            stimulus_parameters["grating_orientation_unit"] = "degrees"

    # The animal's prior-experience descriptors (experience_level, prior_exposures_*) are
    # recorded as stimulus parameters rather than packed into the curriculum_status string,
    # mirroring Visual Behavior Neuropixels (see _TASK_EXPERIENCE_COLUMNS). image_set is not
    # repeated -- it is already this VisualStimulation's stimulus_name.
    for column in _TASK_EXPERIENCE_COLUMNS:
        if column in session_info.index:
            stimulus_parameters[column] = _clean_value(session_info[column])

    # Convert any numpy types to native Python types for serialization
    for key, value in stimulus_parameters.items():
        if isinstance(value, (np.integer, np.floating, np.ndarray)):
            stimulus_parameters[key] = value.tolist()

    visual_stimulation = VisualStimulation(
        stimulus_name=session_info["image_set"],  # e.g., "gratings" or "images_A"
        stimulus_parameters=stimulus_parameters,
        stimulus_template_name=stimulus_template_name,
        notes=None,
    )
    return visual_stimulation


def build_change_detection_stimulus_epoch(
    nwbfile: NWBFile, session_info: pd.Series, session_start_time: datetime
) -> StimulusEpoch:
    """Build the single change-detection ``StimulusEpoch`` for a Visual Behavior session.

    Behavior-only and behavior+ophys sessions run the *same* visual change-detection task,
    with the identical trials / ``task_parameters`` / stimulus-presentation structure in the
    NWB, so the epoch is built the same way for both (this helper is shared by
    ``acquisition_behavior_only`` and ``acquisition_behavior_ophys``). All times are anchored
    to ``session_start_time`` -- the corrected UTC acquisition start (see
    ``utils.get_session_start_time``), not the raw (possibly Pacific-mislabeled) NWB
    ``session_start_time``.

    Parameters
    ----------
    nwbfile : NWBFile
        The session's behavior NWB (for behavior+ophys sessions, any plane file -- the
        behavior data is shared across planes).
    session_info : pd.Series
        Session metadata row (behavior_session_table row for behavior-only sessions,
        ophys_experiment_table row for ophys sessions); both carry ``session_type``,
        ``image_set`` and the curriculum columns.
    session_start_time : datetime
        Corrected UTC acquisition start used to anchor the epoch's start/stop offsets.
    """
    visual_stimulation = get_visual_stimulation(nwbfile, session_info)

    # The change-detection task uses static gratings in the early training stages
    # (TRAINING_0/1/2, image_set == "gratings") and natural images thereafter, so name the
    # epoch by the stimulus it actually used rather than hard-coding "natural images".
    # image_set is the session table's authoritative label (and this VisualStimulation's
    # stimulus_name); dataset-wide it agrees exactly with the NWB carrying a "grating" template
    # / "grating_presentations" intervals table (gratings) vs a natural-images template.
    stimulus_name = (
        "Change detection gratings"
        if session_info["image_set"] == "gratings"
        else "Change detection natural images"
    )

    # The trial start/stop times are seconds relative to the session start. The change
    # detection task spans from the earliest trial (or grating presentation, on early
    # gratings-stage sessions) to the latest.
    start_offset = min(
        nwbfile.intervals['grating_presentations']['start_time'][0]
        if 'grating_presentations' in nwbfile.intervals else nwbfile.trials['start_time'][0],
        nwbfile.trials['start_time'][0],
    )
    stop_offset = max(
        nwbfile.intervals['grating_presentations']['stop_time'][-1]
        if 'grating_presentations' in nwbfile.intervals else nwbfile.trials['stop_time'][-1],
        nwbfile.trials['stop_time'][-1],
    )

    return StimulusEpoch(
        stimulus_start_time=session_start_time + timedelta(seconds=start_offset),
        stimulus_end_time=session_start_time + timedelta(seconds=stop_offset),
        stimulus_name=stimulus_name,
        code=Code(
            url="None",      # stimulus code source not recorded (matches VBN / VC ephys)
            version="None",  # stimulus code version not recorded
            parameters=visual_stimulation,
        ),
        stimulus_modalities=[StimulusModality.VISUAL],
        performance_metrics=None,
        notes=None,
        active_devices=[STIMULUS_MONITOR_NAME],  # the stimulus monitor in the instrument
        configurations=list(),
        # training_protocol_name is left empty (mirrors Visual Behavior Neuropixels): the schema
        # expects it to name a protocol defined in the Procedures, but the Visual Behavior
        # Procedures (surgeries/craniotomies from LIMS) define none. The session stage
        # (session_type, e.g. "OPHYS_1_images_A") is still recorded as the acquisition_type.
        training_protocol_name=None,
        # curriculum_status is left empty: unlike VBN -- whose session_number is a continuous
        # behavioral session count carrying information no other field has -- the Visual Behavior
        # Ophys session_number is just the ophys-stage index (the N in "OPHYS_N"), which is
        # already fully encoded in session_type / acquisition_type, so recording it here would
        # be redundant. The animal's experience descriptors (experience_level, prior_exposures_*)
        # are still carried as stimulus parameters (see get_visual_stimulation).
        curriculum_status=None,
    )


def _passive_epoch_kind(stimulus_name: str) -> str | None:
    """Classify a presentation table's ``stimulus_name`` as a passive (non-task) block.

    Returns ``"spontaneous"`` for the gray-screen blocks, ``"movie"`` for the final natural-movie
    block, or ``None`` for the change-detection *task* table (natural images / gratings), which is
    built separately by :func:`build_change_detection_stimulus_epoch`. These are the only stimulus
    tables the Visual Behavior Ophys NWBs carry.
    """
    if stimulus_name == "spontaneous":
        return "spontaneous"
    if stimulus_name.startswith("natural_movie"):
        return "movie"
    return None


def build_stimulus_epochs(
    nwbfile: NWBFile, session_info: pd.Series, session_start_time: datetime
) -> list[StimulusEpoch]:
    """Build the chronological list of ``StimulusEpoch``s for a Visual Behavior Ophys session.

    Every session has exactly one change-detection *task* block (natural images or gratings),
    emitted with the full task/curriculum metadata by :func:`build_change_detection_stimulus_epoch`.
    The habituation and imaging sessions (``OPHYS_*``) additionally record passive blocks -- the
    pre/mid gray "spontaneous" screens and the final ``natural_movie_one`` block -- each carried in
    its own presentation table with a ``stimulus_block`` column. Those are emitted as one epoch per
    contiguous ``stimulus_block`` (spontaneous spans two: blocks 0 and 2), with **no** task/curriculum
    metadata, mirroring the per-block passive epochs of the VBN / Visual Coding pipelines. Training
    sessions have only the task block, so they yield the single change-detection epoch. The returned
    list is sorted chronologically by start time (as the session unfolded: gray -> task -> gray -> movie).

    Fails loud (raises) if a presentation table cannot be classified or if there is not exactly one
    change-detection task table, so an unexpected NWB layout is surfaced rather than silently dropped.
    """
    epochs = [build_change_detection_stimulus_epoch(nwbfile, session_info, session_start_time)]

    task_tables = 0
    for table_key, table in nwbfile.intervals.items():
        if table_key == "trials":
            continue  # behavioral trials, not a stimulus-presentation table
        df = table.to_dataframe()
        names = df["stimulus_name"].dropna().unique().tolist() if "stimulus_name" in df.columns else []
        if len(names) != 1:
            raise ValueError(
                f"Presentation table {table_key!r} has {len(names)} distinct stimulus_name value(s) "
                f"({names}); expected exactly one to classify the block."
            )
        kind = _passive_epoch_kind(names[0])
        if kind is None:
            task_tables += 1  # the change-detection task block, already built above
            continue
        # One epoch per contiguous stimulus_block (the spontaneous table holds blocks 0 and 2).
        blocks = df.groupby("stimulus_block", sort=True) if "stimulus_block" in df.columns else [(None, df)]
        for _block_id, block_df in blocks:
            epochs.append(
                convert_intervals_to_visual_stimulus_epoch(
                    stimulus_name=names[0].replace("_", " ").title(),  # "Spontaneous" / "Natural Movie One"
                    table_key=table_key,
                    intervals_table=block_df,
                    nwbfile=nwbfile,
                    session_info=None,  # passive block: no training_protocol_name / curriculum_status
                    session_start_time=session_start_time,
                    active_devices=[STIMULUS_MONITOR_NAME],  # the stimulus monitor in the instrument
                )
            )

    if task_tables != 1:
        raise ValueError(
            f"Expected exactly one change-detection task presentation table, found {task_tables}."
        )

    # Chronological order (gray -> change-detection -> gray -> movie for imaging sessions);
    # the passive blocks are built in table order, which is not time order.
    epochs.sort(key=lambda epoch: epoch.stimulus_start_time)
    return epochs


def generate_acquisition(nwbfile: NWBFile, session_info: pd.Series) -> Acquisition:
    """
    Generate an Acquisition model from an NWB file and session metadata.

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing acquisition data
    session_info : pd.Series
        Session metadata row from the behavior session table

    Returns
    -------
    Acquisition
        AIND Acquisition data model populated with data from the NWB file
    """
    # this script is for behavior only files for the visual behavior ophys experiments.
    # get_modalities always adds BEHAVIOR_VIDEOS (eye/body cameras were always recorded).
    assert set(get_modalities(nwbfile)) == {Modality.BEHAVIOR, Modality.BEHAVIOR_VIDEOS}

    subject_id = get_subject_id(nwbfile, session_info=session_info)

    # Corrected UTC acquisition start (the raw NWB session_start_time is Pacific-local
    # mislabeled UTC on the imaging rigs; see utils.get_session_start_time). Every
    # NWB-relative time below is re-anchored to this value.
    session_start_time = get_session_start_time(nwbfile, session_info=session_info)

    # Only include a LickSpoutConfig when a per-reward volume is available. Passive (and
    # other no-reward) sessions deliver no rewards, so get_individual_reward_volume
    # returns None; LickSpoutConfig.volume is a required float and None fails validation,
    # which would fail the whole session. The total reward consumed (0.0 in that case)
    # is still recorded in subject_details below.
    individual_reward_volume = get_individual_reward_volume(nwbfile)
    reward_configs = []
    if individual_reward_volume is not None:
        reward_configs.append(
            LickSpoutConfig(
                device_name="Reward Spout",
                solution=Liquid.WATER,
                solution_valence=Valence.POSITIVE,
                volume=individual_reward_volume,
                volume_unit=VolumeUnit.ML,
                relative_position=["Anterior"],
                notes=get_reward_volume_notes(nwbfile),  # lists all volumes if more than one was used
            )
        )

    # One epoch per contiguous stimulus block, sorted chronologically (behavior-only training
    # sessions have only the change-detection task block; habituation/imaging sessions also
    # carry the gray "spontaneous" and final natural-movie blocks). See build_stimulus_epochs.
    stimulus_epochs = build_stimulus_epochs(nwbfile, session_info, session_start_time)

    acquisition = Acquisition(
        subject_id=subject_id,
        acquisition_start_time=session_start_time,
        acquisition_end_time=get_data_stream_end_time(nwbfile, session_start_time),
        ethics_review_id=get_ethics_review_id(subject_id),
        instrument_id=get_instrument_id(nwbfile, session_info=session_info),
        # session_description is the session-stage string (e.g. "OPHYS_1_images_A",
        # "TRAINING_1_gratings"); confirmed dataset-wide to equal session_info["session_type"]
        # (== the stimulus epoch's training_protocol_name), so it is a consistent acquisition_type.
        acquisition_type=nwbfile.session_description,
        notes=None,
        # Acquisition frame is bregma ARI. NOTE: a behavior-only session can run on any
        # physical rig, and the mesoscope Instrument intentionally declares its own frame as
        # BREGMA_ALS (to stay consistent with other mesoscope experiments), so on a
        # behavior-only mesoscope session the acquisition (ARI) and instrument (ALS) frames
        # differ by design -- see visual_behavior_ophys/instrument.py build_mesoscope_instrument.
        global_coordinate_system=CoordinateSystemLibrary.BREGMA_ARI,
        data_streams=[
            DataStream(
                stream_start_time=get_data_stream_start_time(nwbfile, session_start_time),
                stream_end_time=get_data_stream_end_time(nwbfile, session_start_time),
                modalities=get_modalities(nwbfile),
                code=None,
                notes=None,
                # "Reward Spout" is active only when a reward was delivered (omitted for
                # no-reward sessions, matching the omitted LickSpoutConfig).
                active_devices=[REWARD_SPOUT_NAME] if reward_configs else [],
                configurations=reward_configs,
            ),
        ],
        # TODO - handle different stimulus sets for the different training stages
        stimulus_epochs=stimulus_epochs,
        subject_details=AcquisitionSubjectDetails(
            animal_weight_prior=None,
            animal_weight_post=None,
            weight_unit=MassUnit.G,
            mouse_platform_name=RUNNING_DISC_NAME,  # matches the instrument's Disc
            reward_consumed_total=get_total_reward_volume(nwbfile), # TODO - check if calculation is sufficient
            reward_consumed_unit=VolumeUnit.ML
        ),
    )
    return acquisition
