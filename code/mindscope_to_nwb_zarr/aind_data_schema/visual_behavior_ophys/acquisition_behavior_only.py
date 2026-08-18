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
    get_curriculum_status,
    get_ethics_review_id,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.instrument import (
    REWARD_SPOUT_NAME,
    RUNNING_DISC_NAME,
    STIMULUS_MONITOR_NAME,
)


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
        stimulus_name="Change detection natural images",
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
        training_protocol_name=session_info["session_type"],  # e.g., "TRAINING_0_gratings_autorewards_15min"
        curriculum_status=get_curriculum_status(session_info),
    )


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
        stimulus_epochs=[
            build_change_detection_stimulus_epoch(nwbfile, session_info, session_start_time),
        ],
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
