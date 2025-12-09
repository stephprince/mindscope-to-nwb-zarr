"""Generates an example JSON file for visual behavior behavior-ophys acquisition"""

from datetime import datetime, timezone, timedelta
import json
import re

import numpy as np
import pynwb

from aind_data_schema.components.identifiers import Software, Code
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
    Translation,
    Rotation,
    AtlasCoordinate,
    AtlasLibrary,
    CoordinateSystemLibrary,
    Scale,
)
from aind_data_schema.components.stimulus import VisualStimulation, OptoStimulation
from aind_data_schema_models.units import TimeUnit, SizeUnit, VolumeUnit, FrequencyUnit, MassUnit, PowerUnit
from aind_data_schema_models.brain_atlas import CCFv3
from aind_data_schema_models.stimulus_modality import StimulusModality

import pandas as pd
from pynwb import read_nwb
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
    serialized_dict,
)

# example file for initial debugging
# TODO - replace with more general ingestion/generation script
subject_id = 403491
nwbfile_session_id = "20181129T093257"  # example behavior+ophys file, single plane ophys
nwbfile = read_nwb(f"C:/Users/Ryan/Documents/mindscope-to-nwb-zarr/data/sub-{subject_id}_ses-{nwbfile_session_id}_image+ophys.nwb")
ophys_experiment_id = int(nwbfile.identifier)  # e.g., 788490510, which corresponds to behavior_session_id 788017709, ophys_session_id 787661032, ophys_container_id 782536745
ophys_experiment_table = pd.read_csv("C:/Users/Ryan/Documents/mindscope-to-nwb-zarr/data/visual_behavior_ophys_metadata/ophys_experiment_table.csv")
session_info = ophys_experiment_table.query("mouse_id == @subject_id and ophys_experiment_id == @ophys_experiment_id")
assert len(session_info) == 1, f"Expected exactly one matching session info entry, instead found {len(session_info)}"

# this script is for behavior only files for the visual behavior ophys experiments
from aind_data_schema_models.modalities import Modality
assert set(get_modalities(nwbfile)) == set([Modality.POPHYS, Modality.BEHAVIOR])

assert len(nwbfile.devices) == 1
device = next(iter(nwbfile.devices.values()))
assert re.match("CAM2P\.\d", device.name)
assert device.description == "Allen Brain Observatory - Scientifica 2P Rig"
assert device.manufacturer == "Scientifica"


assert len(nwbfile.imaging_planes) == 1, "Expected single plane imaging"
imaging_plane = next(iter(nwbfile.imaging_planes.values()))
assert imaging_plane.name == "imaging_plane_1"
assert imaging_plane.indicator == "GCaMP6f"
assert imaging_plane.excitation_lambda == 910
assert imaging_plane.imaging_rate == 31

# TODO where to store the tissue emission lambda in the AIND metadata?
assert len(imaging_plane.optical_channel) == 1
assert imaging_plane.optical_channel[0].description == "2P Optical Channel"
assert imaging_plane.optical_channel[0].emission_lambda == 520  # nm


# example: imaging_plane.description = "(447, 512) field of view in VISp at depth 375 um"

imaging_plane_description_pattern = "\((\d+), (\d+)\) field of view in (\w+) at depth (\d+) um"
imaging_plane_description_re_match = re.search(imaging_plane_description_pattern, imaging_plane.description)
assert imaging_plane_description_re_match, f"Imaging plane description does not match expected pattern: {imaging_plane.description}"

# NOTE: This is not always (512, 512) as described in the white paper
imaging_plane_dimensions = [int(imaging_plane_description_re_match.group(1)), int(imaging_plane_description_re_match.group(2))]
imaging_plane_targeted_structure_str = imaging_plane_description_re_match.group(3)
imaging_plane_targeted_structure = CCFv3.by_acronym(imaging_plane_targeted_structure_str)
imaging_plane_depth = int(imaging_plane_description_re_match.group(4))

# cross-check with custom metadata object in the NWB file
ophys_behavior_metadata = nwbfile.lab_meta_data["metadata"]  # neurodata_type: OphysBehaviorMetadata
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
assert device.name == ophys_behavior_metadata.equipment_name
assert imaging_plane_dimensions == [ophys_behavior_metadata.field_of_view_width, ophys_behavior_metadata.field_of_view_height]
assert imaging_plane_depth == ophys_behavior_metadata.imaging_depth
assert imaging_plane_depth == ophys_behavior_metadata.targeted_imaging_depth
assert ophys_behavior_metadata.imaging_plane_group == -1
assert ophys_behavior_metadata.imaging_plane_group_count == 0
assert ophys_behavior_metadata.project_code == "VisualBehavior"
assert ophys_behavior_metadata.ophys_container_id == session_info["ophys_container_id"].values[0]
assert ophys_behavior_metadata.ophys_experiment_id == session_info["ophys_experiment_id"].values[0]
assert ophys_behavior_metadata.ophys_session_id == session_info["ophys_session_id"].values[0]
assert ophys_behavior_metadata.session_type == session_info["session_type"].values[0]

# cross-check these with the ophys_experiment_table.csv
assert imaging_plane_depth == session_info["imaging_depth"].values[0]
assert imaging_plane_depth == session_info["targeted_imaging_depth"].values[0]
assert imaging_plane_targeted_structure_str == session_info["targeted_structure"].values[0]
assert np.isnan(session_info["imaging_plane_group"].values[0])
assert session_info["project_code"].values[0] == "VisualBehavior"
assert re.match("CAM2P\.\d", session_info["equipment_name"].values[0])
assert device.name == session_info["equipment_name"].values[0]

# TODO can the microscope name have spaces? the examples all replace spaces with underscores
microscope_name = f"Scientifica VivoScope 2P Rig ({device.name})"

acquisition = Acquisition(
    subject_id=get_subject_id(nwbfile, session_info=session_info),
    specimen_id=None,
    acquisition_start_time=get_session_start_time(nwbfile, session_info=session_info),
    acquisition_end_time=get_data_stream_end_time(nwbfile),
    # experimenters=None, # TODO - determine where to extract
    protocol_id=None,
    ethics_review_id=None,  # TODO get from Saskia
    instrument_id=get_instrument_id(nwbfile, session_info=session_info),
    acquisition_type=nwbfile.session_description, # TODO - confirm consistent across experiments or if better option
    notes=None,
    coordinate_system=CoordinateSystemLibrary.BREGMA_ARID, # TODO - determine correct system library
    # instrument and acquisition do not have the same coordinate system. 
    # For Ophys, it will define the location of the imaging FOV in a way that can be entered. Saskia will check.
    # calibrations=None,  # will be difficult to find, so leave out
    # maintenance=None,
    data_streams=[
        DataStream(
            stream_start_time=get_data_stream_start_time(nwbfile),
            stream_end_time=get_data_stream_end_time(nwbfile),
            modalities=get_modalities(nwbfile),
            code=None,
            notes=None,
            active_devices=[  # Instruments need to be defined
                microscope_name,
                "Lick_Spout_1",  # placeholder
            ],
            configurations=[
                # See Visual Behavior Technical White Paper
                # SECTION E: IN VIVO 2-PHOTON CALCIUM IMAGING. HARDWARE & INSTRUMENTATION
                # which often references methods from de Vries et al., 2020
                
                ImagingConfig(
                    device_name=microscope_name,
                    channels=[
                        Channel(
                            channel_name="Green channel",
                            intended_measurement=imaging_plane.indicator,
                            detector=DetectorConfig(
                                device_name="PMT 1", # TODO: A PMT seems to be used, but I cannot find these parameters
                                exposure_time=0.1, 
                                trigger_type=TriggerType.INTERNAL,
                            ),
                            light_sources=[
                                LaserConfig(
                                    device_name="Laser A (Titanium-sapphire laser (Chameleon Vision, Coherent))",
                                    wavelength=imaging_plane.excitation_lambda,
                                    wavelength_unit=SizeUnit.NM,
                                    power=None,  # TODO was this information stored anywhere?? "Once a depth location was stabilized, a combination of PMT gain and laser power was selected to maximize laser power (based on a look-up table against depth) and dynamic range while avoiding pixel saturation." (de Vries et al., 2020)
                                    power_unit=None,  # TODO ^
                                    # TODO should this be recorded: Pulse dispersion compensation / pre-compensation = ~10,000 fs2
                                ),
                            ],
                        ),
                    ],
                    images=[
                        PlanarImage(
                            channel_name="Green channel",
                            image_to_acquisition_transform=[
                                # Translation(
                                #     translation=[1.5, 1.5],
                                # ),  # TODO - where to find this information?
                            ],
                            dimensions=Scale(
                                scale=imaging_plane_dimensions
                            ),
                            planes=[
                                Plane(
                                    depth=imaging_plane_depth,
                                    depth_unit=SizeUnit.UM,
                                    power=5,  # TODO: What is this? Maximum signal intensity deterioration from original intensity level??? Probably not.
                                    power_unit=PowerUnit.PERCENT,  # TODO ^
                                    targeted_structure=imaging_plane_targeted_structure,
                                ),
                            ],
                        ),
                    ],
                    sampling_strategy=SamplingStrategy(
                        frame_rate=imaging_plane.imaging_rate,
                        frame_rate_unit=FrequencyUnit.HZ,
                    ),
                ),
                LickSpoutConfig(  # Lick spout is specific to the rig
                    device_name="Lick_Spout_1",  # placeholder
                    solution=Liquid.WATER,
                    solution_valence=Valence.POSITIVE,
                    volume=get_individual_reward_volume(nwbfile), # TODO - what to do if multiple? this does happen
                    volume_unit=VolumeUnit.ML,
                    relative_position=["Anterior"], # TODO - what is the correct information here? It looks like the relative position was determined per-subject and this is not stored anywhere
                )
            ],
         ),
    ],
    # TODO - handle different stimulus sets for the different training stages
    stimulus_epochs=[

    ], 
    # manipulations=None, # TODO - think this is None (seems to be injections)
    subject_details=AcquisitionSubjectDetails(
        animal_weight_prior=None,
        animal_weight_post=None,
        weight_unit=MassUnit.G,
        anaesthesia=None,
        mouse_platform_name="Running Wheel", # TODO - determine where to extract if needed
        reward_consumed_total=get_total_reward_volume(nwbfile), # TODO - check if calculation is sufficient
        reward_consumed_unit=VolumeUnit.ML
    ),
)


if __name__ == "__main__":
    serialized = acquisition.model_dump_json()
    deserialized = Acquisition.model_validate_json(serialized)
    deserialized.write_standard_file(prefix="ophys_visual_behavior")