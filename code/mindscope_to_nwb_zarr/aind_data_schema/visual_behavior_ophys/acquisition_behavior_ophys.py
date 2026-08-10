"""Generates acquisition metadata for visual behavior ophys behavior+ophys sessions"""

import re
from typing import Any

import numpy as np
import pandas as pd
from pynwb import NWBFile
from pynwb.ophys import ImagingPlane

from aind_data_schema.core.acquisition import (
    Acquisition,
    DataStream,
    AcquisitionSubjectDetails,
)
from aind_data_schema.components.configs import (
    LickSpoutConfig,
    Liquid,
    Valence,
    Channel,
    DetectorConfig,
    LaserConfig,
    TriggerType,
    ImagingConfig,
    CoupledPlane,
    Plane,
    PlanarImage,
    SamplingStrategy,
)
from aind_data_schema.components.coordinates import (
    CoordinateSystemLibrary,
    Scale,
)
from aind_data_schema_models.units import SizeUnit, VolumeUnit, FrequencyUnit, MassUnit, PowerUnit, TimeUnit
from aind_data_schema_models.brain_atlas import CCFv3
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
    get_ethics_review_id,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.instrument import (
    DETECTOR_NAME,
    LASER_NAME,
    STIMULUS_MONITOR_NAME,
    microscope_name_for_equipment,
    ophys_device_names,
)


def process_nwb_imaging_plane(nwbfile: NWBFile, session_info: pd.Series, is_single_plane: bool) -> dict[str, Any]:
    """Check and process the imaging plane from an NWB file and extract metadata that changes across planes.

    Check that the imaging plane matches expected values for visual behavior behavior-ophys sessions,
    and extract metadata such as dimensions, targeted structure, and depth.

    Args:
        nwbfile: The NWB file to process.
        session_info: Session metadata row from the session table.
        is_single_plane: Whether the NWB file is from a single-plane ophys session.

    Returns:
        A dictionary containing imaging plane metadata:
            imaging_plane: The imaging plane object.
            imaging_plane_dimensions: The dimensions of the imaging plane.
            imaging_plane_targeted_structure: The targeted brain structure of the imaging plane.
            imaging_plane_targeted_structure_str: The targeted brain structure as a string.
            imaging_plane_depth: The depth of the imaging plane.
    """
    assert len(nwbfile.devices) == 1
    device = next(iter(nwbfile.devices.values()))

    assert len(nwbfile.imaging_planes) == 1, "Expected one plane per NWB file"
    imaging_plane = next(iter(nwbfile.imaging_planes.values()))

    assert imaging_plane.name == "imaging_plane_1"
    assert imaging_plane.indicator in ("GCaMP6f", "GCaMP6s")
    assert imaging_plane.excitation_lambda == 910
    assert imaging_plane.location is not None
    assert imaging_plane.description is not None

    if is_single_plane:
        assert imaging_plane.imaging_rate == 31
    else:   
        assert imaging_plane.imaging_rate == 11

    assert len(imaging_plane.optical_channel) == 1
    assert imaging_plane.optical_channel[0].description == "2P Optical Channel"
    assert imaging_plane.optical_channel[0].emission_lambda == 520  # nm

    # NOTE: for multi-plane sessions, the imaging plane in each NWB file has a different description
    imaging_plane_description_pattern = r"\((\d+), (\d+)\) field of view in (\w+) at depth (\d+) um"
    imaging_plane_description_re_match = re.search(imaging_plane_description_pattern, imaging_plane.description)
    assert imaging_plane_description_re_match, f"Imaging plane description does not match expected pattern: {imaging_plane.description}"

    # NOTE: This is not always (512, 512) as described in the white paper
    imaging_plane_dimensions = [int(imaging_plane_description_re_match.group(1)), int(imaging_plane_description_re_match.group(2))]
    if is_single_plane:
        # Single-plane FOV height is always 512; the width is (roughly) rig-specific and
        # varies across sessions (observed 447, 449, 451, 452 for CAM2P.5/.4/.3), so
        # validate structurally rather than against a fixed allowlist.
        assert imaging_plane_dimensions[1] == 512 and 0 < imaging_plane_dimensions[0] <= 512, \
            f"Unexpected imaging plane dimensions for single-plane NWB file: {imaging_plane_dimensions}"
    else:
        assert imaging_plane_dimensions == [512, 512], f"Unexpected imaging plane dimensions for multi-plane NWB file: {imaging_plane_dimensions}"

    imaging_plane_targeted_structure_str = imaging_plane_description_re_match.group(3)
    imaging_plane_targeted_structure = CCFv3.by_acronym(imaging_plane_targeted_structure_str)
    imaging_plane_depth = int(imaging_plane_description_re_match.group(4))

    assert imaging_plane.location == imaging_plane_targeted_structure_str

    # Check that the plane-specific metadata in each NWB file matches the expected values
    # from elsewhere in the NWB file and from session_info
    # TODO: Store all of these values in the Acquisition JSON? or elsewhere??
    ophys_behavior_metadata = nwbfile.lab_meta_data["metadata"]  # neurodata_type: OphysBehaviorMetadata
    # Example values from a single-plane NWB file:
    # behavior_session_uuid	"bdc41492-1797-4c91-81ba-18fc0a25d238"
    # equipment_name	"CAM2P.5"
    # field_of_view_height	512
    # field_of_view_width	447
    # imaging_depth	375
    # imaging_plane_group	-1
    # imaging_plane_group_count	0
    # ophys_container_id	782536745
    # ophys_experiment_id	788490510
    # ophys_session_id	787661032
    # project_code	"VisualBehavior"
    # session_type	"OPHYS_6_images_B"
    # stimulus_frame_rate	60
    # targeted_imaging_depth	375

    # TODO For visual coding, a container is all the data collected from the same FoV.
    # Current schema - no concept of a "session" but useful to maintain this information for continuity
    # Put in notes in data description field. Same for container id
    # There is "tags" field in the data description. Use that for container id.

    # For Visual Behavior
    # Put experiment ID in the data stream notes for now

    assert ophys_behavior_metadata.equipment_name == device.name

    if is_single_plane:
        assert re.match(r"CAM2P\.\d", session_info["equipment_name"])
    else:
        assert session_info["equipment_name"] == "MESO.1"
    assert session_info["equipment_name"] == device.name

    assert [
        ophys_behavior_metadata.field_of_view_width,
        ophys_behavior_metadata.field_of_view_height
    ] == imaging_plane_dimensions

    assert ophys_behavior_metadata.imaging_depth == imaging_plane_depth
    assert imaging_plane_depth == session_info["imaging_depth"]

    if is_single_plane:
        assert ophys_behavior_metadata.imaging_plane_group == -1
        assert ophys_behavior_metadata.imaging_plane_group_count == 0
        assert np.isnan(session_info["imaging_plane_group"])
    else:
        # the imaging_plane_group (0-indexed) is used to group coupled planes together
        assert ophys_behavior_metadata.imaging_plane_group >= 0
        assert ophys_behavior_metadata.imaging_plane_group_count == 4
        assert session_info["imaging_plane_group"] == ophys_behavior_metadata.imaging_plane_group

    assert ophys_behavior_metadata.ophys_container_id == session_info["ophys_container_id"]
    assert ophys_behavior_metadata.ophys_experiment_id == session_info["ophys_experiment_id"]
    assert ophys_behavior_metadata.ophys_session_id == session_info["ophys_session_id"]
    if is_single_plane:
        assert ophys_behavior_metadata.project_code in ("VisualBehavior", "VisualBehaviorTask1B")
    else:
        assert ophys_behavior_metadata.project_code in ("VisualBehaviorMultiscope", "VisualBehaviorMultiscope4areasx2d")
    assert ophys_behavior_metadata.project_code == session_info["project_code"]
    assert ophys_behavior_metadata.session_type == session_info["session_type"]

    assert ophys_behavior_metadata.stimulus_frame_rate == 60

    assert ophys_behavior_metadata.targeted_imaging_depth == session_info["targeted_imaging_depth"]

    # also cross-check other values from the NWB file with the session info
    assert imaging_plane_targeted_structure_str == session_info["targeted_structure"]

    return dict(
        device=device,
        imaging_plane=imaging_plane,
        imaging_plane_dimensions=imaging_plane_dimensions,
        imaging_plane_targeted_structure=imaging_plane_targeted_structure,
        imaging_plane_targeted_structure_str=imaging_plane_targeted_structure_str,
        imaging_plane_depth=imaging_plane_depth,
        imaging_plane_group=int(ophys_behavior_metadata.imaging_plane_group) if not is_single_plane else None,
    )



def create_imaging_config(microscope_name: str, imaging_plane: ImagingPlane, dimensions: list[int], planes: list[Plane]) -> ImagingConfig:
    """Generates an imaging configuration for a visual behavior behavior-ophys acquisition.

    Works for both single-plane and multi-plane acquisitions.

    """
    
    imaging_config = ImagingConfig(
        device_name=microscope_name,
        channels=[
            Channel(
                channel_name="Green channel",
                intended_measurement=imaging_plane.indicator,
                detector=DetectorConfig(
                    device_name=DETECTOR_NAME,  # "PMT" in the instrument (2P + mesoscope)
                    exposure_time=0.1,
                    trigger_type=TriggerType.INTERNAL,
                ),
                light_sources=[
                    LaserConfig(
                        device_name=LASER_NAME,  # "Ti-Saph" in the instrument
                        wavelength=imaging_plane.excitation_lambda,
                        wavelength_unit=SizeUnit.NM,
                        power=None,  # NOTE: Laser power was adjusted per session and was not recorded in the NWB files
                    ),
                ],
                emission_filters=[],
                emission_wavelength=imaging_plane.optical_channel[0].emission_lambda,
                emission_wavelength_unit=SizeUnit.NM,
            ),
        ],
        images=[
            PlanarImage(
                channel_name="Green channel",  # should match one of the defined channels above
                image_to_acquisition_transform=[
                    # Translation(
                    #     translation=[1.5, 1.5],
                    # ),  # TODO - where to find this information? Saskia will follow up with the team
                    # May not have this information anymore. For visual behavior multi-plane imaging,
                    # probably want to represent this.
                ],
                dimensions=Scale(
                    scale=dimensions
                ),
                planes=planes,
            ),
        ],
        sampling_strategy=SamplingStrategy(
            frame_rate=imaging_plane.imaging_rate,
            frame_rate_unit=FrequencyUnit.HZ,
        ),
    )
    
    return imaging_config


def get_single_plane_imaging_config(microscope_name: str, imaging_plane_info: dict) -> ImagingConfig:
    """Generates an imaging configuration for a single-plane visual behavior behavior-ophys acquisition.
    
    Args:
        microscope_name: The name of the microscope used for imaging.
        imaging_plane_info: A dictionary containing imaging plane metadata for a single plane.

    Returns:
        An ImagingConfig object representing the imaging configuration for the plane.
    """
    imaging_plane: ImagingPlane = imaging_plane_info["imaging_plane"]
    imaging_plane_dimensions: list[int] = imaging_plane_info["imaging_plane_dimensions"]
    imaging_plane_targeted_structure: CCFv3 = imaging_plane_info["imaging_plane_targeted_structure"]
    imaging_plane_depth: int = imaging_plane_info["imaging_plane_depth"]

    planes = [
        Plane(
            depth=imaging_plane_depth,
            depth_unit=SizeUnit.UM,
            power=-1,  # TODO Add laser power (required). Might have this information for multi-plane imaging for visual behavior because there is power sharing @Saskia
            power_unit=PowerUnit.PERCENT,  # TODO This is also required. See above comment.
            targeted_structure=imaging_plane_targeted_structure,
        ),
    ]

    return create_imaging_config(microscope_name, imaging_plane, imaging_plane_dimensions, planes)


def get_multiplane_imaging_config(microscope_name: str, imaging_plane_info_all: list[dict]) -> ImagingConfig:
    """Generates imaging configuration for a multi-plane (coupled planes) visual behavior behavior-ophys acquisition.

    See Visual Behavior Technical White Paper
    SECTION E: IN VIVO 2-PHOTON CALCIUM IMAGING. HARDWARE & INSTRUMENTATION
    which often references methods from de Vries et al., 2020

    Args:
        microscope_name: The name of the microscope used for imaging.
        imaging_plane_info: A list of dictionaries containing imaging plane metadata:
            imaging_plane: The imaging plane object.
            imaging_plane_dimensions: The dimensions of the imaging plane.
            imaging_plane_targeted_structure: The targeted brain structure of the imaging plane.
            imaging_plane_depth: The depth of the imaging plane.

    Returns:
        An ImagingConfig object representing the imaging configuration for the plane.
    """

    # Sanity check that all planes have the same basic parameters
    first_imaging_plane_info = imaging_plane_info_all[0]
    for imaging_plane_info in imaging_plane_info_all[1:]:
        assert imaging_plane_info["device"].name == first_imaging_plane_info["device"].name
        assert imaging_plane_info["imaging_plane"].imaging_rate == first_imaging_plane_info["imaging_plane"].imaging_rate
        assert imaging_plane_info["imaging_plane"].indicator == first_imaging_plane_info["imaging_plane"].indicator
        assert imaging_plane_info["imaging_plane"].excitation_lambda == first_imaging_plane_info["imaging_plane"].excitation_lambda
        assert imaging_plane_info["imaging_plane"].optical_channel[0].emission_lambda == first_imaging_plane_info["imaging_plane"].optical_channel[0].emission_lambda
        assert imaging_plane_info["imaging_plane_dimensions"] == first_imaging_plane_info["imaging_plane_dimensions"]

    # Split planes into groups of coupled planes
    # Visual behavior multi-plane ophys has 4 groups of coupled planes
    grouped_imaging_planes: list[list[dict]] = [list() for _ in range(4)]
    for imaging_plane_info in imaging_plane_info_all:
        # NOTE: order of planes within a group is arbitrary
        imaging_plane_group = imaging_plane_info["imaging_plane_group"]
        grouped_imaging_planes[imaging_plane_group].append(imaging_plane_info)
    for imaging_plane_group in grouped_imaging_planes:
        count = len(imaging_plane_group)
        assert count <= 2, f"Expected at most 2 planes per imaging plane group, instead found {count} for group {imaging_plane_group}"

    imaging_plane: ImagingPlane = first_imaging_plane_info["imaging_plane"]
    imaging_plane_dimensions: list[int] = first_imaging_plane_info["imaging_plane_dimensions"]

    planes = list()
    for group_index, imaging_plane_group in enumerate(grouped_imaging_planes):
        if len(imaging_plane_group) == 0:
            continue  # skip empty groups
        if len(imaging_plane_group) == 1:
            # TODO create dummy coupled plane - could infer from other days. Indicate in the closest Notes that plane X failed QC and is therefore not in the data.
            # only one plane in this group, so just add it as a regular Plane
            first_plane_index = group_index * 2
            imaging_plane_depth = imaging_plane_group[0]["imaging_plane_depth"]
            imaging_plane_targeted_structure = imaging_plane_group[0]["imaging_plane_targeted_structure"]
            plane = CoupledPlane(
                depth=imaging_plane_depth,
                depth_unit=SizeUnit.UM,
                power=-1,  # TODO Add laser power (required). Might have this information for multi-plane imaging for visual behavior because there is power sharing @Saskia
                power_unit=PowerUnit.PERCENT,  # TODO This is also required. See above comment.
                targeted_structure=imaging_plane_targeted_structure,
                plane_index=first_plane_index,
                coupled_plane_index=-1,  # TODO what to put here if there is no coupled plane? @Saskia
                power_ratio=1.0,  # TODO Add power ratio (required) based on number of planes and power sharing. @Saskia
            )
            planes.append(plane)
        else:
            first_plane_index = group_index * 2
            second_plane_index = first_plane_index + 1
            imaging_plane_depth = imaging_plane_group[0]["imaging_plane_depth"]
            imaging_plane_targeted_structure = imaging_plane_group[0]["imaging_plane_targeted_structure"]
            plane = CoupledPlane(
                depth=imaging_plane_depth,
                depth_unit=SizeUnit.UM,
                power=-1,  # TODO Add laser power (required). Might have this information for multi-plane imaging for visual behavior because there is power sharing @Saskia
                power_unit=PowerUnit.PERCENT,  # TODO This is also required. See above comment.
                targeted_structure=imaging_plane_targeted_structure,
                plane_index=first_plane_index,
                coupled_plane_index=second_plane_index,
                power_ratio=1.0,  # TODO Add power ratio (required) based on number of planes and power sharing. @Saskia
            )
            planes.append(plane)

            # Second plane of the coupled pair: use the *second* plane's own depth and
            # targeted structure (coupled planes are imaged simultaneously at two
            # different depths, so this must index [1], not [0]).
            imaging_plane_depth = imaging_plane_group[1]["imaging_plane_depth"]
            imaging_plane_targeted_structure = imaging_plane_group[1]["imaging_plane_targeted_structure"]
            plane = CoupledPlane(
                depth=imaging_plane_depth,
                depth_unit=SizeUnit.UM,
                power=-1,  # TODO Add laser power (required). Might have this information for multi-plane imaging for visual behavior because there is power sharing @Saskia
                power_unit=PowerUnit.PERCENT,  # TODO This is also required. See above comment.
                targeted_structure=imaging_plane_targeted_structure,
                plane_index=second_plane_index,  # NOTE this is swapped compared to the first plane above
                coupled_plane_index=first_plane_index,
                power_ratio=1.0,  # TODO Add power ratio (required) based on number of planes and power sharing. @Saskia
            )
            planes.append(plane)

    return create_imaging_config(microscope_name, imaging_plane, imaging_plane_dimensions, planes)


def generate_acquisition(nwbfiles: list[NWBFile], session_infos: list[pd.Series]) -> Acquisition:
    """
    Generate an Acquisition model from NWB file(s) and session metadata.

    Parameters
    ----------
    nwbfiles : list[NWBFile]
        List of NWB files containing acquisition data. For single-plane sessions,
        this list contains one file. For multiplane sessions, this list contains
        one file per imaging plane.
    session_infos : list[pd.Series]
        List of session metadata rows from the session table, one per NWB file.

    Returns
    -------
    Acquisition
        AIND Acquisition data model populated with data from the NWB file(s)
    """
    assert len(nwbfiles) == len(session_infos), "Must have one session_info per NWB file"
    assert len(nwbfiles) >= 1, "Must have at least one NWB file"

    # Use first NWB file for shared metadata (behavior data is the same across all planes)
    nwbfile = nwbfiles[0]
    session_info = session_infos[0]

    # this script is for behavior + ophys sessions for the visual behavior ophys project.
    # get_modalities always adds BEHAVIOR_VIDEOS (eye/body cameras were always recorded).
    assert set(get_modalities(nwbfile)) == {Modality.POPHYS, Modality.BEHAVIOR, Modality.BEHAVIOR_VIDEOS}

    assert len(nwbfile.devices) == 1
    device = next(iter(nwbfile.devices.values()))

    # The instrument names the microscope per-rig (e.g. "Scientifica 1", "Multiscope"),
    # not by equipment id; map the NWB device name (== equipment_name) to that name so the
    # ImagingConfig.device_name points at the instrument's microscope component.
    microscope_name = microscope_name_for_equipment(device.name)  # e.g. "Scientifica 1" or "Multiscope"

    # Determine if single-plane or multi-plane based on device
    if re.match(r"CAM2P\.\d", device.name):
        # single-plane ophys sessions use the Scientifica rig
        assert len(nwbfiles) == 1, "Single-plane sessions should have exactly one NWB file"
        is_single_plane = True
        assert device.description == "Allen Brain Observatory - Scientifica 2P Rig"
        assert device.manufacturer == "Scientifica"
        imaging_plane_info = process_nwb_imaging_plane(nwbfile, session_info, is_single_plane)
        imaging_config = get_single_plane_imaging_config(microscope_name, imaging_plane_info)
    elif device.name == "MESO.1":
        # multi-plane ophys sessions use the Mesoscope rig
        is_single_plane = False
        assert device.description == "Allen Brain Observatory - Mesoscope 2P Rig"
        assert device.manufacturer is None

        # Process all plane NWB files
        imaging_plane_info_all = []
        for nwbfile_plane, session_info_plane in zip(nwbfiles, session_infos):
            imaging_plane_info = process_nwb_imaging_plane(nwbfile_plane, session_info_plane, is_single_plane)
            imaging_plane_info_all.append(imaging_plane_info)
        imaging_config = get_multiplane_imaging_config(microscope_name, imaging_plane_info_all)
    else:
        raise ValueError(f"Unknown device: {device.name}")

    # Device names to reference in the configs below, matching the instrument built for
    # this rig (single-plane 2P vs mesoscope); see instrument.ophys_device_names.
    device_names = ophys_device_names(is_single_plane)
    behavior_video_devices = [device_names["body_camera"], device_names["eye_camera"]]
    if "face_camera" in device_names:  # mesoscope also has a face camera
        behavior_video_devices.append(device_names["face_camera"])

    subject_id = get_subject_id(nwbfile, session_info=session_info)

    # Only include a LickSpoutConfig when a per-reward volume is available. Passive (and
    # other no-reward) ophys sessions deliver no rewards, so get_individual_reward_volume
    # returns None; LickSpoutConfig.volume is a required float and None fails validation,
    # which would fail the whole session. The total reward consumed (0.0 in that case)
    # is still recorded in subject_details below.
    individual_reward_volume = get_individual_reward_volume(nwbfile)
    reward_configs = []
    if individual_reward_volume is not None:
        reward_configs.append(
            LickSpoutConfig(  # Lick spout is specific to the rig; see the reward spout / solenoid in the instrument
                device_name="Reward Spout",
                solution=Liquid.WATER,
                solution_valence=Valence.POSITIVE,
                volume=individual_reward_volume,
                volume_unit=VolumeUnit.ML,
                relative_position=["Anterior"],
                notes="",  # TODO - write that reward volume was both x and y
            )
        )

    acquisition = Acquisition(
        subject_id=subject_id,
        specimen_id=None,
        acquisition_start_time=get_session_start_time(nwbfile, session_info=session_info),
        acquisition_end_time=get_data_stream_end_time(nwbfile),
        # protocol.io DOI is not recorded in these NWB files (nwbfile.protocol is None
        # for all VB sub-experiment types); keep None until a protocol id is available.
        protocol_id=[nwbfile.protocol] if nwbfile.protocol else None,
        ethics_review_id=get_ethics_review_id(subject_id),
        instrument_id=get_instrument_id(nwbfile, session_info=session_info),
        acquisition_type=nwbfile.session_description,
        notes=None,
        global_coordinate_system=CoordinateSystemLibrary.BREGMA_ARID, # TODO - determine correct system library. depends on the transform
        # instrument and acquisition do not have the same coordinate system. 
        # For Ophys, it will define the location of the imaging FOV in a way that can be entered. Saskia will check.
        data_streams=[
            DataStream(
                stream_start_time=get_data_stream_start_time(nwbfile),
                stream_end_time=get_data_stream_end_time(nwbfile),
                modalities=get_modalities(nwbfile),
                code=None,
                notes=None,
                active_devices=[
                    microscope_name,
                    device_names["laser"],     # excitation laser ("Ti-Saph")
                    device_names["detector"],  # detector ("PMT")
                    STIMULUS_MONITOR_NAME,     # visual stimulus screen
                    *behavior_video_devices,
                    device_names["reward_spout"],
                ],
                configurations=[
                    imaging_config,
                    # No DetectorConfig is fabricated for the behavior cameras: the NWB
                    # does not record a true camera exposure time, and the cameras' frame
                    # rates are already on the instrument (30 Hz single-plane / 60 Hz
                    # mesoscope).
                    *reward_configs,  # LickSpoutConfig, only when a reward volume exists
                ],
            ),
        ],
        # TODO - handle different stimulus sets for the different training stages
        stimulus_epochs=[
            # TODO consult StimulusEpoch objects from acquisition_visual_behavior_ophys_behavior.py
        ], 
        subject_details=AcquisitionSubjectDetails(
            animal_weight_prior=None,
            animal_weight_post=None,
            weight_unit=MassUnit.G,
            anaesthesia=None,
            mouse_platform_name=device_names["running_disc"],  # matches the instrument's Disc
            reward_consumed_total=get_total_reward_volume(nwbfile),
            reward_consumed_unit=VolumeUnit.ML
        ),
    )

    return acquisition
