"""Generates acquisition metadata from NWB files for visual coding ophys sessions"""

import numpy as np
import re
import pandas as pd

from datetime import timedelta
from functools import lru_cache
from pathlib import Path
from pynwb import NWBFile

from aind_data_schema.components.identifiers import Code, Software
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
from aind_data_schema.components.stimulus import VisualStimulation
from aind_data_schema_models.units import SizeUnit, FrequencyUnit, MassUnit, PowerUnit
from aind_data_schema_models.brain_atlas import CCFv3
from aind_data_schema_models.stimulus_modality import StimulusModality

from mindscope_to_nwb_zarr.pynwb_utils import (
    get_data_stream_start_time,
    get_data_stream_end_time,
    get_modalities
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ophys.instrument import (
    rig_for_experiment,
    MICROSCOPE_NAME,
)


# CSV mapping subject (donor/mouse) id -> ethics (IACUC) review id, bundled in the repo.
_ETHICS_REVIEW_CSV = Path(__file__).resolve().parents[3] / "reference" / "ethics_review_ids.csv"


@lru_cache(maxsize=None)
def _load_subject_to_ethics_review(csv_path: str) -> dict:
    """Load the subject_id -> ethics_review_id mapping from the CSV (cached)."""
    df = pd.read_csv(csv_path, usecols=["subject_id", "ethics_review_id"])
    return dict(zip(df["subject_id"].astype(int), df["ethics_review_id"].astype(int)))


def get_ethics_review_id(subject_id, csv_path=None) -> list[str]:
    """Look up the ethics review id(s) for a subject.

    Parameters
    ----------
    subject_id : int | str
        The subject's donor/mouse id (e.g. ``"244896"``).
    csv_path : str | Path, optional
        Path to the subject->ethics_review_id mapping CSV. Defaults to the bundled copy.

    Returns
    -------
    list[str]
        A single-element list with the ethics review id as a string (the
        Acquisition field is a list).

    Raises
    ------
    KeyError
        If the subject is not present in the mapping CSV.
    """
    mapping = _load_subject_to_ethics_review(str(csv_path or _ETHICS_REVIEW_CSV))
    review_id = mapping.get(int(subject_id))
    if review_id is None:
        raise KeyError(
            f"No ethics_review_id found for subject_id {subject_id!r} in {_ETHICS_REVIEW_CSV}"
        )
    return [str(review_id)]


def get_imaging_plane_info(nwbfile: NWBFile, session_info: pd.Series) -> dict:
    """Extract imaging plane metadata from an NWB file.

    Args:
        nwbfile: The NWB file to process.
        session_info: Session metadata row from the ophys experiment metadata.

    Returns:
        A dictionary containing imaging plane metadata.
    """
    assert len(nwbfile.devices) == 3, "Expected three devices per NWB file: Camera, Microscope, and StimulusDisplay"
    device = nwbfile.devices["Microscope"]

    assert len(nwbfile.imaging_planes) == 1, "Expected one imaging plane per NWB file"
    imaging_plane = next(iter(nwbfile.imaging_planes.values()))

    imaging_plane_dimensions = [512, 512]  # Default dimensions
    imaging_plane_depth = session_info.get('imaging_depth')

    targeted_structure_str = imaging_plane.location
    assert targeted_structure_str == session_info['targeted_structure']['acronym'], (
        f"Imaging plane targeted structure '{targeted_structure_str}' does not match session info "
        f"'{session_info['targeted_structure']['acronym']}'"
    )

    # Get CCFv3 brain structure
    targeted_structure = CCFv3.by_acronym(targeted_structure_str)

    return dict(
        device=device,
        imaging_plane=imaging_plane,
        imaging_plane_dimensions=imaging_plane_dimensions,
        imaging_plane_targeted_structure=targeted_structure,
        imaging_plane_targeted_structure_str=targeted_structure_str,
        imaging_plane_depth=imaging_plane_depth,
    )


def _get_emission_wavelength(imaging_plane) -> float | None:
    """Get emission wavelength from imaging plane, returning None if not available or nan."""
    if not imaging_plane.optical_channel:
        return None
    emission_lambda = imaging_plane.optical_channel[0].emission_lambda
    if emission_lambda is None or (isinstance(emission_lambda, float) and np.isnan(emission_lambda)):
        return None
    return float(emission_lambda)


def create_imaging_config(nwbfile: NWBFile, imaging_plane_info: dict) -> ImagingConfig:
    """Create an imaging configuration for a visual coding ophys acquisition.

    Args:
        nwbfile: The NWB file to process.
        imaging_plane_info: Dictionary containing imaging plane metadata.

    Returns:
        An ImagingConfig object representing the imaging configuration.
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
        device_name=MICROSCOPE_NAME,  # matches the microscope device defined in the instrument file
        channels=[
            Channel(
                channel_name="Green channel",
                intended_measurement=imaging_plane.indicator,
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


def get_stimulus_epochs(nwbfile: NWBFile, session_info: pd.Series) -> list[StimulusEpoch]:
    """Extract stimulus epochs from NWB file intervals tables.

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing intervals tables
    session_info : pd.Series
        Session metadata row

    Returns
    -------
    list[StimulusEpoch]
        List of stimulus epochs extracted from the NWB file
    """
    stimulus_epochs = []

    # Visual Coding ophys files have stimulus presentations in intervals
    for table_key, intervals_table in nwbfile.intervals.items():
        # Skip non-stimulus tables
        if table_key in ["trials", "invalid_times"]:
            continue

        intervals_df = intervals_table.to_dataframe()
        if len(intervals_df) == 0:
            continue

        # Convert table key to formatted stimulus name
        stimulus_name = table_key.replace('_', ' ').title()

        # Extract stimulus parameters
        stimulus_parameters = {}
        for col in intervals_df.columns:
            if col not in ['start_time', 'stop_time', 'id']:
                unique_values = intervals_df[col].unique().tolist()
                if len(unique_values) == 1:
                    stimulus_parameters[col] = unique_values[0]
                else:
                    stimulus_parameters[col] = unique_values

        visual_stim = VisualStimulation(
            stimulus_name=table_key,
            stimulus_parameters=stimulus_parameters,
            stimulus_template_name=[],
            notes=None,
        )

        stim_epoch = StimulusEpoch(
            stimulus_start_time=timedelta(seconds=intervals_df['start_time'].values[0]) + nwbfile.session_start_time,
            stimulus_end_time=timedelta(seconds=intervals_df['stop_time'].values[-1]) + nwbfile.session_start_time,
            stimulus_name=stimulus_name,
            code=Code(
                url="None",
                core_dependency=Software(name="PsychoPy", version=None),
                parameters=visual_stim.model_dump(),
            ),
            stimulus_modalities=[StimulusModality.VISUAL],
            notes=None,
            active_devices=["Stimulus Screen"],  # the stimulus monitor in the instrument
            performance_metrics=None,
            training_protocol_name=None,
            curriculum_status=None,
        )
        stimulus_epochs.append(stim_epoch)

    return stimulus_epochs


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
    device = imaging_plane_info["device"]

    # Create imaging config
    imaging_config = create_imaging_config(nwbfile, imaging_plane_info)

    # Get subject ID from session metadata (external_donor_name is the 6-digit mouse ID)
    subject_id = session_info['specimen']['donor']['external_donor_name']

    acquisition = Acquisition(
        subject_id=subject_id,
        specimen_id=None,
        acquisition_start_time=nwbfile.session_start_time,
        acquisition_end_time=get_data_stream_end_time(nwbfile),
        protocol_id=[nwbfile.protocol],  # TODO is this correct? Example value 20160706_244896_3StimC @Saskia
        ethics_review_id=get_ethics_review_id(subject_id),
        # Match the instrument file's instrument_id (the rig name, e.g. "CAM2P.1").
        # Falls back to the NWB device name for sessions whose rig is unresolved.
        instrument_id=rig_for_experiment(session_info) or device.name,
        acquisition_type=session_info.get('stimulus_name', 'Visual Coding 2p'),
        notes=None,
        coordinate_system=CoordinateSystemLibrary.BREGMA_ARI,
        data_streams=[
            DataStream(
                stream_start_time=get_data_stream_start_time(nwbfile),
                stream_end_time=get_data_stream_end_time(nwbfile),
                modalities=get_modalities(nwbfile),
                code=None,
                notes=None,
                # Device names must match devices defined in the instrument file.
                # Per de Vries et al. (2020), each session simultaneously recorded the
                # two-photon movie, eye tracking, a side-view full-body camera, and
                # running speed -- all at 30 Hz.
                active_devices=[
                    MICROSCOPE_NAME,           # two-photon microscope
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
            animal_weight_prior=None,  # NOTE: Animal weight was not recorded
            animal_weight_post=None,
            weight_unit=MassUnit.G,
            anaesthesia=None,
            mouse_platform_name="MindScope Running Disc",  # matches the Disc device in the instrument; de Vries et al. describe a rotating disk
        ),
    )

    return acquisition
