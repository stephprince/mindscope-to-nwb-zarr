"""Generates acquisition metadata from NWB files for visual coding ophys sessions"""

import re
import pandas as pd

from datetime import timedelta
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
from aind_data_schema_models.units import SizeUnit, FrequencyUnit, MassUnit, PowerUnit, TimeUnit
from aind_data_schema_models.brain_atlas import CCFv3
from aind_data_schema_models.stimulus_modality import StimulusModality

from mindscope_to_nwb_zarr.pynwb_utils import (
    get_data_stream_start_time,
    get_data_stream_end_time,
    get_modalities
)


def get_imaging_plane_info(nwbfile: NWBFile, session_info: pd.Series) -> dict:
    """Extract imaging plane metadata from an NWB file.

    Args:
        nwbfile: The NWB file to process.
        session_info: Session metadata row from the ophys experiment metadata.

    Returns:
        A dictionary containing imaging plane metadata.
    """
    assert len(nwbfile.devices) == 1
    device = next(iter(nwbfile.devices.values()))

    assert len(nwbfile.imaging_planes) == 1, "Expected one imaging plane per NWB file"
    imaging_plane = next(iter(nwbfile.imaging_planes.values()))

    # Parse imaging plane description for dimensions and structure
    # Format: "(width, height) field of view in STRUCTURE at depth X um"
    description_pattern = r"\((\d+), (\d+)\) field of view in (\w+) at depth (\d+) um"
    match = re.search(description_pattern, imaging_plane.description)

    if match:
        imaging_plane_dimensions = [int(match.group(1)), int(match.group(2))]
        targeted_structure_str = match.group(3)
        imaging_plane_depth = int(match.group(4))
    else:
        # Fall back to session_info if description doesn't match
        imaging_plane_dimensions = [512, 512]  # Default
        targeted_structure_str = session_info.get('targeted_structure', 'VISp')
        imaging_plane_depth = int(session_info.get('imaging_depth', 175))

    # Get CCFv3 brain structure
    try:
        targeted_structure = CCFv3.by_acronym(targeted_structure_str)
    except (ValueError, AttributeError):
        targeted_structure = CCFv3.VIS  # Fall back to generic visual area

    return dict(
        device=device,
        imaging_plane=imaging_plane,
        imaging_plane_dimensions=imaging_plane_dimensions,
        imaging_plane_targeted_structure=targeted_structure,
        imaging_plane_targeted_structure_str=targeted_structure_str,
        imaging_plane_depth=imaging_plane_depth,
    )


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
    device = imaging_plane_info["device"]

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
        device_name=device.name,  # TODO: The device name will need to match the device name defined in the instrument file @Saskia
        channels=[
            Channel(
                channel_name="Green channel",
                intended_measurement=imaging_plane.indicator,
                detector=DetectorConfig(
                    device_name="PMT 1",  # TODO: This should correspond to a device in the instrument file @Saskia
                    exposure_time=0.1,
                    trigger_type=TriggerType.INTERNAL,
                ),
                light_sources=[
                    LaserConfig(
                        device_name="Ti:Sapphire laser",  # TODO: This should correspond to a device in the instrument file @Saskia
                        wavelength=imaging_plane.excitation_lambda,
                        wavelength_unit=SizeUnit.NM,
                        power=None,  # NOTE: Laser power was adjusted per session and was not recorded in the NWB files
                    ),
                ],
                emission_filters=[],
                emission_wavelength=imaging_plane.optical_channel[0].emission_lambda if imaging_plane.optical_channel else 520,
                emission_wavelength_unit=SizeUnit.NM,
            ),
        ],
        images=[
            PlanarImage(
                channel_name="Green channel",  # should match one of the defined channels above
                image_to_acquisition_transform=[
                    # TODO: Where to find this information? May not have this information anymore. @Saskia
                ],
                dimensions=Scale(scale=imaging_plane_dimensions),
                planes=planes,
            ),
        ],
        sampling_strategy=SamplingStrategy(
            frame_rate=imaging_plane.imaging_rate,
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
            active_devices=["None"],
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
        protocol_id=None,
        ethics_review_id=None,  # TODO @Saskia
        instrument_id=device.name,  # TODO: The instrument ID will need to match the instrument file @Saskia
        acquisition_type=session_info.get('stimulus_name', 'Visual Coding 2p'),
        notes=None,
        coordinate_system=CoordinateSystemLibrary.BREGMA_ARID,  # TODO: Determine correct coordinate system @Saskia
        data_streams=[
            DataStream(
                stream_start_time=get_data_stream_start_time(nwbfile),
                stream_end_time=get_data_stream_end_time(nwbfile),
                modalities=get_modalities(nwbfile),
                code=None,
                notes=None,
                active_devices=[  # TODO: These device names need to match the instrument file @Saskia
                    device.name,
                    "BehaviorCamera",
                    "EyeCamera",
                ],
                configurations=[
                    imaging_config,
                    DetectorConfig(
                        device_name="BehaviorCamera",  # TODO: This should correspond to a device in the instrument file @Saskia
                        exposure_time=33,
                        exposure_time_unit=TimeUnit.MS,
                        trigger_type=TriggerType.INTERNAL,
                    ),
                ],
            ),
        ],
        stimulus_epochs=get_stimulus_epochs(nwbfile, session_info),
        subject_details=AcquisitionSubjectDetails(
            animal_weight_prior=None,  # NOTE: Animal weight was not recorded
            animal_weight_post=None,
            weight_unit=MassUnit.G,
            anaesthesia=None,
            mouse_platform_name="Running Wheel",  # TODO: Verify this is correct for visual coding ophys @Saskia
        ),
    )

    return acquisition
