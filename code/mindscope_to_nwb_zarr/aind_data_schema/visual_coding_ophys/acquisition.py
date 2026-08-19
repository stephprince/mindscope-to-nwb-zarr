"""Generates acquisition metadata from NWB files for visual coding ophys sessions"""

import numpy as np
import pandas as pd

from pynwb import NWBFile
from pynwb.image import IndexSeries
from pynwb.epoch import TimeIntervals

from aind_data_schema.core.acquisition import (
    Acquisition,
    StimulusEpoch,
    DataStream,
    AcquisitionSubjectDetails,
)
from aind_data_schema.components.configs import (
    Channel,
    DetectorConfig,
    LaserConfig,
    Translation,
    TriggerType,
    ImagingConfig,
    Plane,
    PlanarImage,
    SamplingStrategy,
)
from aind_data_schema.components.coordinates import (
    CoordinateSystemLibrary,
    Scale,
)
from aind_data_schema_models.units import SizeUnit, FrequencyUnit, PowerUnit
from aind_data_schema_models.brain_atlas import CCFv3

from mindscope_to_nwb_zarr.pynwb_utils import (
    get_data_stream_start_time,
    get_data_stream_end_time,
    get_modalities,
    reconstruct_stimulus_epochs_table,
)
from mindscope_to_nwb_zarr.aind_data_schema.utils import (
    get_ethics_review_id,
    convert_intervals_to_visual_stimulus_epoch,
    warn_if_too_few_presentations,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ophys.instrument import (
    rig_for_experiment,
    microscope_name_for_experiment,
)


# Field-of-view dimensions [height, width] in pixels. The Allen Brain Observatory
# two-photon rigs acquire a fixed 512 x 512 pixel frame (de Vries et al., 2020).
# Hard-coded as a dataset constant because the raw acquisition FOV is not recorded in
# the processed NWB file. This may not match the summary images in the NWB file (e.g.
# maximum_intensity_projection), which can be cropped by motion registration.
IMAGING_FOV_DIMENSIONS = [512, 512]


# Expected presentation counts for the parameterized (DynamicTable) stimuli in the Visual
# Coding 2P protocol (de Vries et al., 2020). Used to warn when an NWB stimulus table is
# truncated -- some files store only a few static_gratings rows instead of the full ~6000
# individual grating presentations. Only DynamicTable stimuli are listed here; natural
# scenes and movies are IndexSeries (frame-indexed), handled via num_frames/num_repeats.
EXPECTED_PRESENTATIONS = {
    "static_gratings": 6000,   # 6 orientations x 5 spatial frequencies x 4 phases x 50 + blank sweeps
    "drifting_gratings": 628,  # 8 directions x 5 temporal frequencies x 15 + blank sweeps
}


def get_imaging_plane_info(nwbfile: NWBFile, session_info: pd.Series) -> dict:
    """Extract imaging plane metadata from an NWB file.

    Parameters
    ----------
    nwbfile : NWBFile
        The NWB file to process.
    session_info : pd.Series
        Session metadata row from the ophys experiment metadata.

    Returns
    -------
    dict
        Imaging plane metadata (imaging plane, dimensions, targeted structure, depth).
    """
    assert len(nwbfile.devices) == 3, "Expected three devices per NWB file: Camera, Microscope, and StimulusDisplay"

    assert len(nwbfile.imaging_planes) == 1, "Expected one imaging plane per NWB file"
    imaging_plane = next(iter(nwbfile.imaging_planes.values()))

    imaging_plane_dimensions = IMAGING_FOV_DIMENSIONS  # fixed dataset FOV; see module constant
    imaging_plane_depth = session_info['imaging_depth']

    targeted_structure_str = imaging_plane.location
    assert targeted_structure_str == session_info['targeted_structure']['acronym'], (
        f"Imaging plane targeted structure '{targeted_structure_str}' does not match session info "
        f"'{session_info['targeted_structure']['acronym']}'"
    )

    # Get CCFv3 brain structure
    targeted_structure = CCFv3.by_acronym(targeted_structure_str)

    return dict(
        imaging_plane=imaging_plane,
        imaging_plane_dimensions=imaging_plane_dimensions,
        imaging_plane_targeted_structure=targeted_structure,
        imaging_plane_targeted_structure_str=targeted_structure_str,
        imaging_plane_depth=imaging_plane_depth,
    )


def _get_emission_wavelength(imaging_plane) -> float | None:
    """Get the emission wavelength (nm) from the imaging plane's optical channel.

    The emission wavelength is not recorded in these NWB files (it is stored as NaN
    for every session, unlike ``excitation_lambda`` which is 910 nm), so this returns
    ``None`` -- a known-missing value, like the laser power. See the README.
    """
    if not imaging_plane.optical_channel:
        return None
    emission_lambda = imaging_plane.optical_channel[0].emission_lambda
    if emission_lambda is None or (isinstance(emission_lambda, float) and np.isnan(emission_lambda)):
        return None
    return float(emission_lambda)


def create_imaging_config(nwbfile: NWBFile, imaging_plane_info: dict, microscope_name: str) -> ImagingConfig:
    """Create an imaging configuration for a visual coding ophys acquisition.

    Parameters
    ----------
    nwbfile : NWBFile
        The NWB file to process.
    imaging_plane_info : dict
        Dictionary containing imaging plane metadata.
    microscope_name : str
        Name of the instrument's microscope device this config points at (per-rig,
        e.g. "Nikon 1"); see ``microscope_name_for_experiment``.

    Returns
    -------
    ImagingConfig
        The imaging configuration.
    """
    imaging_plane = imaging_plane_info["imaging_plane"]
    imaging_plane_dimensions = imaging_plane_info["imaging_plane_dimensions"]
    imaging_plane_depth = imaging_plane_info["imaging_plane_depth"]
    targeted_structure = imaging_plane_info["imaging_plane_targeted_structure"]

    planes = [
        Plane(
            depth=imaging_plane_depth,
            depth_unit=SizeUnit.UM,
            power=-1,  # NOTE: Laser power was adjusted per session and was not recorded in the NWB files
            power_unit=PowerUnit.PERCENT,
            targeted_structure=targeted_structure,
        ),
    ]

    imaging_config = ImagingConfig(
        device_name=microscope_name,  # matches the microscope device defined in the instrument file
        channels=[
            Channel(
                channel_name="Green channel",
                intended_measurement=imaging_plane.indicator,
                # Laser power was adjusted per session and not recorded, but we do
                # not mark the channel as variable-power (kept False, the default).
                variable_power=False,
                detector=DetectorConfig(
                    device_name="PMT",  # Corresponds to device in instrument file
                    # No exposure time: the imaging is resonant-scanner two-photon at
                    # 30 Hz (de Vries et al., 2020), recorded via SamplingStrategy below.
                    # A PMT is a point detector with no camera-style exposure time, and
                    # the paper/whitepaper report only the 30 Hz frame rate.
                    trigger_type=TriggerType.INTERNAL,
                ),
                light_sources=[
                    LaserConfig(
                        device_name="Ti-Saph",  # Corresponds to device in instrument file
                        wavelength=imaging_plane.excitation_lambda,
                        wavelength_unit=SizeUnit.NM,
                        power=None,  # NOTE: Laser power was adjusted per session and was not recorded in the NWB files
                    ),
                ],
                emission_filters=[],
                emission_wavelength=_get_emission_wavelength(imaging_plane),
                emission_wavelength_unit=SizeUnit.NM,
            ),
        ],
        images=[
            PlanarImage(
                channel_name="Green channel",  # Matches defined channel above
                image_to_acquisition_transform=[
                    Translation(translation=[0, 0])
                ],
                dimensions=Scale(scale=imaging_plane_dimensions),
                planes=planes,
            ),
        ],
        sampling_strategy=SamplingStrategy(
            frame_rate=30,  # from de Vries et al, 2019
            frame_rate_unit=FrequencyUnit.HZ,
        ),
    )

    return imaging_config


def get_stimulus_epochs(nwbfile: NWBFile, session_info: pd.Series | None = None) -> list[StimulusEpoch]:
    """Extract stimulus epochs from NWB file intervals tables.

    Visual Coding ophys NWBs store all stimulus presentations in a single ``epochs``
    intervals table, one row per contiguous stimulus block (a ``stimulus_type`` can
    recur in several non-contiguous blocks across the session). This emits one
    ``StimulusEpoch`` per block, each carrying that block's own start/stop time from
    the NWB file, via the shared ``convert_intervals_to_visual_stimulus_epoch`` helper.

    For the 34 sessions whose DANDI file has **no** epochs table (the upstream converter
    skipped them because AllenSDK's ``get_stimulus_epoch_table`` raised
    ``EpochSeparationException``), the epochs table is reconstructed from ``nwb.stimulus``
    via :func:`reconstruct_stimulus_epochs_table` (which fails loudly on any anomaly). The
    ophys experiment id needed to locate the static_gratings cache comes from ``session_info``.

    The helper is called with ``session_info=None`` because the ophys experiment
    metadata has no ``session_type`` (training protocol / curriculum status stay
    ``None``); the stimulus monitor is recorded as the epoch's active device.

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing intervals tables
    session_info : pandas.Series, optional
        The experiment row; its ``id`` is used to locate the static_gratings cache when the
        epochs table must be reconstructed.

    Returns
    -------
    list[StimulusEpoch]
        List of per-block stimulus epochs extracted from the NWB file
    """
    stimulus_epochs = []

    # Visual Coding ophys stores all stimulus blocks in the "epochs" intervals table. When
    # that table is absent (34 sessions), reconstruct it from the per-stimulus presentation
    # times so these sessions get stimulus epochs like every other session.
    if nwbfile.epochs is not None:
        epoch_dataframes = [
            table.to_dataframe()
            for key, table in nwbfile.intervals.items()
            if key not in ("trials", "invalid_times")
        ]
    else:
        experiment_id = int(session_info["id"]) if session_info is not None and "id" in session_info else None
        reconstructed = reconstruct_stimulus_epochs_table(nwbfile, experiment_id=experiment_id)
        print(
            f"[epochs] source file has no epochs table; reconstructed {len(reconstructed)} "
            f"stimulus epoch blocks from nwb.stimulus."
        )
        epoch_dataframes = [reconstructed.to_dataframe()]

    for intervals_df in epoch_dataframes:
        # One epoch per row/block, so each has its own start/stop. The block's stimulus_type
        # names the epoch and selects its metadata annotation from nwb.stimulus.
        for i in range(len(intervals_df)):
            block_df = intervals_df.iloc[[i]]
            block_name = str(block_df['stimulus_type'].iloc[0])
            start_time = float(block_df['start_time'].iloc[0])
            stop_time = float(block_df['stop_time'].iloc[0])
            parameters, template_name, notes = get_block_stimulus_annotation(
                nwbfile, block_name, start_time, stop_time
            )
            stim_epoch = convert_intervals_to_visual_stimulus_epoch(
                stimulus_name=block_name.replace('_', ' ').title(),
                table_key=block_name,
                intervals_table=block_df,
                nwbfile=nwbfile,
                session_info=None,  # ophys metadata has no session_type/curriculum fields
                active_devices=["Stimulus Screen"],  # the stimulus monitor in the instrument
                extra_parameters=parameters,
                stimulus_template_name=template_name,
                notes=notes,
            )
            stimulus_epochs.append(stim_epoch)

    # Return the epochs in chronological order (they are built per stimulus block, which is
    # not necessarily time order).
    stimulus_epochs.sort(key=lambda epoch: epoch.stimulus_start_time)

    return stimulus_epochs


# Small tolerance for matching stimulus presentations to a block's [start, stop] window.
_BLOCK_TIME_EPS = 1e-6


def get_block_stimulus_annotation(nwbfile: NWBFile, stimulus_type: str,
                                  start_time: float, stop_time: float) -> tuple[dict, list, str]:
    """Collect available metadata for one stimulus block from its ``nwb.stimulus`` entry.

    A Visual Coding ophys ``epochs`` row only carries ``stimulus_type`` and the block's
    start/stop. The richer per-stimulus metadata lives in the matching ``nwb.stimulus``
    object (a ``TimeIntervals`` of parameterized presentations for gratings/spontaneous,
    or an ``IndexSeries`` of template frame indices for natural scenes/movies). This
    gathers what is available for the block, restricted to its time window.

    Parameters
    ----------
    nwbfile : NWBFile
        The NWB file, whose ``stimulus`` group holds the per-stimulus objects.
    stimulus_type : str
        The block's stimulus_type (e.g. ``"static_gratings"``, ``"natural_scenes"``).
    start_time, stop_time : float
        The block's start/stop (seconds relative to session start) used to window the
        presentations that belong to this block.

    Returns
    -------
    tuple[dict, list, str]
        ``(parameters, stimulus_template_name, notes)``:
        - ``parameters``: stimulus parameters for the block -- the unique value(s) of each
          presentation column within the window (e.g. grating orientation,
          spatial_frequency, phase) plus a presentation/frame count.
        - ``stimulus_template_name``: single-element list with the referenced template
          name for template-indexed stimuli (``IndexSeries``), else ``[]``.
        - ``notes``: the stimulus object's description.

    Raises
    ------
    ValueError
        If ``stimulus_type`` has no matching ``nwb.stimulus`` object.
    TypeError
        If the matching object is neither a ``TimeIntervals`` nor an ``IndexSeries``.
    """
    # The epochs table uses bare stimulus_type names; some nwb.stimulus keys add a
    # "_stimulus" suffix (e.g. "natural_scenes" -> "natural_scenes_stimulus").
    for key in (stimulus_type, f"{stimulus_type}_stimulus"):
        if key in nwbfile.stimulus:
            stimulus = nwbfile.stimulus[key]
            break
    else:
        raise ValueError(
            f"No nwb.stimulus object for stimulus_type {stimulus_type!r} "
            f"(looked for {stimulus_type!r} and {stimulus_type + '_stimulus'!r})."
        )

    notes = stimulus.description
    parameters: dict = {}
    stimulus_template_name: list = []

    if isinstance(stimulus, TimeIntervals):
        presentations = stimulus.to_dataframe()
        # Warn if this parameterized stimulus table is truncated (fewer presentations than
        # the protocol delivered). Checked against the full table, not the per-block window.
        warn_if_too_few_presentations(stimulus_type, len(presentations), EXPECTED_PRESENTATIONS)
        in_block = presentations[
            (presentations["start_time"] >= start_time - _BLOCK_TIME_EPS)
            & (presentations["stop_time"] <= stop_time + _BLOCK_TIME_EPS)
        ]
        parameters["num_presentations"] = int(len(in_block))
        for column in in_block.columns:
            if column in ("start_time", "stop_time"):
                continue
            values = in_block[column].unique().tolist()
            parameters[column] = values[0] if len(values) == 1 else values
    elif isinstance(stimulus, IndexSeries):
        timestamps = np.asarray(stimulus.timestamps[:])
        in_block = (timestamps >= start_time - _BLOCK_TIME_EPS) & (timestamps <= stop_time + _BLOCK_TIME_EPS)
        num_presented = int(in_block.sum())
        if stimulus.indexed_timeseries is not None:
            # Movie (indexed via an ImageSeries): num_frames is the movie length -- the
            # number of distinct frames shown in this block -- and num_repeats is how many
            # times the movie was shown in this block. A movie can span several epoch
            # blocks (e.g. natural_movie_three is two blocks of 5 repeats), so both are
            # per-block; num_frames * num_repeats == the frames presented in the block.
            frame_indices = np.asarray(stimulus.data[:])[in_block]
            num_frames = int(np.unique(frame_indices).size)
            parameters["num_frames"] = num_frames
            if num_frames:
                parameters["num_repeats"] = num_presented // num_frames
        else:
            # Natural scenes / sparse noise (indexed via Images): not repeat-structured,
            # so report the number of frame presentations in this epoch.
            parameters["num_frames"] = num_presented
        # The referenced template (Images for natural scenes, ImageSeries for movies).
        template = stimulus.indexed_timeseries or stimulus.indexed_images
        stimulus_template_name = [template.name]
    else:
        raise TypeError(
            f"Unexpected nwb.stimulus type {type(stimulus).__name__} for stimulus_type "
            f"{stimulus_type!r}; expected TimeIntervals or IndexSeries."
        )

    return parameters, stimulus_template_name, notes


def generate_acquisition(nwbfile: NWBFile, session_info: pd.Series) -> Acquisition:
    """
    Generate an Acquisition model from an NWB file and session metadata.

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing acquisition data
    session_info : pd.Series
        Session metadata row from the ophys experiment metadata

    Returns
    -------
    Acquisition
        AIND Acquisition data model populated with data from the NWB file
    """
    # Extract imaging plane info
    imaging_plane_info = get_imaging_plane_info(nwbfile, session_info)

    # Per-rig microscope device name (e.g. "Nikon 1"), matching the instrument file.
    microscope_name = microscope_name_for_experiment(session_info)

    # Create imaging config
    imaging_config = create_imaging_config(nwbfile, imaging_plane_info, microscope_name)

    # Get subject ID from session metadata (external_donor_name is the 6-digit mouse ID)
    subject_id = session_info['specimen']['donor']['external_donor_name']

    # The data stream end (last data timestamp) is also the acquisition end.
    end_time = get_data_stream_end_time(nwbfile)

    acquisition = Acquisition(
        subject_id=subject_id,
        specimen_id=None,
        acquisition_start_time=nwbfile.session_start_time,
        acquisition_end_time=end_time,
        protocol_id=[nwbfile.protocol],  # e.g., 20160706_244896_3StimC
        ethics_review_id=get_ethics_review_id(subject_id),
        # The rig name (e.g. "CAM2P.1"), matching the generated Instrument's instrument_id.
        instrument_id=rig_for_experiment(session_info),
        # The Allen "session_type": the stimulus_name from the experiment metadata
        # (e.g. "three_session_C"). No separate session_type field exists in the schema.
        acquisition_type=session_info['stimulus_name'],
        notes=None,
        global_coordinate_system=CoordinateSystemLibrary.BREGMA_ARI,
        data_streams=[
            DataStream(
                stream_start_time=get_data_stream_start_time(nwbfile),
                stream_end_time=end_time,
                modalities=get_modalities(nwbfile),
                code=None,
                notes=None,
                # Device names must match devices defined in the instrument file.
                # Per de Vries et al. (2020), each session simultaneously recorded the
                # two-photon movie, eye tracking, a side-view full-body camera, and
                # running speed -- all at 30 Hz.
                active_devices=[
                    microscope_name,           # two-photon microscope
                    "Ti-Saph",                 # excitation laser
                    "PMT",                     # detector
                    "Eye Camera",              # eye-tracking camera
                    "Body Camera",             # side-view full-body camera
                    "MindScope Running Disc",  # running wheel (running speed)
                ],
                # Only the imaging config is included. The behavior camera's 30 fps is
                # already recorded on the instrument's Camera (frame_rate=30 Hz); the
                # NWB does not record a true camera exposure time, so no DetectorConfig
                # is fabricated for it (a 33 ms "exposure" would just be the frame period).
                configurations=[
                    imaging_config,
                ],
            ),
        ],
        stimulus_epochs=get_stimulus_epochs(nwbfile, session_info),
        subject_details=AcquisitionSubjectDetails(
            mouse_platform_name="MindScope Running Disc",  # matches the Disc device in the instrument; de Vries et al. describe a rotating disk
        ),
    )

    return acquisition
