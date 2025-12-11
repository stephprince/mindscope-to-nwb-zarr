"""Generates an example JSON file for visual coding ephys acquisition"""

import numpy as np
import pandas as pd
import warnings

from datetime import timedelta
from pathlib import Path
from pynwb import read_nwb

from aind_data_schema.components.identifiers import Software, Code
from aind_data_schema.core.acquisition import (
    Acquisition,
    StimulusEpoch,
    DataStream,
    AcquisitionSubjectDetails,
)
from aind_data_schema.components.configs import (
    ManipulatorConfig,
    EphysAssemblyConfig,
    ProbeConfig,
    LaserConfig,
)
from aind_data_schema.components.coordinates import (
    Translation,
    AtlasCoordinate,
    AtlasLibrary,
    CoordinateSystemLibrary,
)
from aind_data_schema.components.stimulus import VisualStimulation, PulseShape
from aind_data_schema_models.units import TimeUnit, SizeUnit, MassUnit
from aind_data_schema_models.brain_atlas import CCFv3
from aind_data_schema_models.stimulus_modality import StimulusModality

from mindscope_to_nwb_zarr.aind_data_schema.custom_stimulus import OptotaggingStimulation
from mindscope_to_nwb_zarr.pynwb_utils import (
    get_data_stream_start_time,
    get_data_stream_end_time, 
    get_modalities
)
from mindscope_to_nwb_zarr.aind_data_schema.utils import (
    get_brain_locations,
)

# example file for initial debugging
# TODO - replace with more general ingestion/generation script
# TODO - allensdk metadata more difficult for this file, need to cross-check or no?

repo_root = Path(__file__).parent.parent.parent.parent
cache_dir = repo_root / ".cache/visual_coding_ephys_cache_dir/"
subject_id = "744912845"
session_id = "766640955"
nwbfile = read_nwb(repo_root / f"data/sub-{subject_id}_ses-{session_id}.nwb")

# load metadata files
def get_probe_configs(nwbfile):
    probe_configs = []
    all_targeted_structures = []
    for device in nwbfile.devices.values():
        if device.__class__.__name__ == "EcephysProbe":
            # TODO - determine how to map 12 visual areas in coding dataset to CCFv3
            # for debugging purposes mapping to a general VIS structure
            # VISal, VISam, VISl, VISpl, VISp, VISrl, VISpm 
            # (need to be mapped: VISm, VISli, VISlla, VISm, VISmma, VISmmp) 
            all_structures = get_brain_locations(nwbfile, device)
            targeted_structure = [s for s in all_structures if s.acronym.startswith('VIS')] # get targeted visual area
            if len(targeted_structure) > 1:
                warnings.warn(f"More than one visual area found: {targeted_structure}")
            all_targeted_structures.append(targeted_structure[0])

            probe_configs.append(
                ProbeConfig(
                    device_name=device.name,
                    # 6 probes, each targets a cortical visual area (e.g. VISp, VISl, VISal, VISrl, VISam, VISpm)
                    # would list that specific area as the primary targeted structure 
                    # should be the same for every experiment, most files should have majority of one
                    primary_targeted_structure=targeted_structure[0],
                    other_targeted_structure=list(set(all_structures) - set(targeted_structure)), # TODO - currently listing all other structures that are hit but might want to not list everything
                    atlas_coordinate=AtlasCoordinate(
                        coordinate_system=AtlasLibrary.CCFv3_10um,
                        translation=[0, 0, 0], # TODO - should be target region coordinate - might not make sense for these datasets, TBD @Saskia
                    ),
                    coordinate_system=CoordinateSystemLibrary.MPM_MANIP_RFB, # TODO - what should this be? probably bregma ARID, will confirm
                    transform=[Translation(translation=[0, 0, 0, 1],),], # TODO - what should this be? this will be the translation we care about, how we've positioned this probe
                    # expect that there is documentation on these translations somewhere @Saskia
                    notes=None,
                )
            )
    assert len(set(all_targeted_structures)) == len(all_targeted_structures), "Duplicate targeted structures found across probes"
    
    return probe_configs

def get_optostimulation_parameters(optogenetic_stimulation):
    opto_stimulation = dict()
    opto_df = optogenetic_stimulation.to_dataframe()
    for stimulus_name, df in opto_df.groupby('stimulus_name'):
        assert len(df['condition'].unique()) == 1, "Multiple pulse shapes found for stimulus_name"
        if 'pulse' in df['condition'].values[0]:
            pulse_shape = PulseShape.SQUARE # TODO - double check if this is best descriptor for both slow and fast pulses
        elif 'cosine' in df['condition'].values[0]:
            pulse_shape = PulseShape.RAMP # TODO - described as "raised cosine ramp" in whitepaper, could define new enum if needed

        # get pulse duration and light levels used
        light_levels = sorted(df['level'].unique().tolist())
        pulse_durations = df['duration'].unique()

        opto_stimulation[stimulus_name] = OptotaggingStimulation(
            stimulus_name=stimulus_name,
            pulse_shape=pulse_shape,
            pulse_durations=[np.round(p, 10) for p in pulse_durations],
            pulse_durations_unit=TimeUnit.S,
            ramp_duration=0.0005,
            ramp_duration_unit=TimeUnit.S,
            inter_pulse_interval=1.5,
            inter_pulse_interval_unit=TimeUnit.S,
            inter_pulse_interval_delay_range=(0, 0.5),
            inter_pulse_interval_delay_range_unit=TimeUnit.S,
            light_levels=light_levels,
            condition_description=df['condition'].values[0],
        )

    return opto_stimulation

def get_visual_stimulation_parameters(table_key: str, intervals_table: pd.DataFrame) -> VisualStimulation:
    # TODO - determine if there are any other parameters to include
    possible_parameters_and_units = {"orientation": "degrees",
                                     "spatial_frequency": "cycles/degree",
                                     "temporal_frequency": "Hz",
                                     "contrast": "percent",
                                     "duration": "S",
                                     "phase": None,
                                     "size": None,
                                     "stimulus_name": None,
                                     "stimulus_block": None,
                                     "color": None,
                                     "opacity": None,
                                     "mask": None,
                                     "speed": "degrees/second",
                                     "dir": "degrees",
                                     "coherence": "percent",
                                     "dotLife": None,
                                     "dotSize": None,
                                     "nDots": None,
                                     "fieldPos": None,
                                     "fieldShape": None,
                                     "fieldSize": None,
                                     } # TODO - determine if any of these have better units
    parameters = {}
    for param_key, param_unit in possible_parameters_and_units.items():
        if param_key in intervals_table.columns:
            parameter_values = intervals_table[param_key].unique().tolist()
            parameter_values = parameter_values[0] if len(parameter_values) == 1 else parameter_values
            parameters.update({param_key: parameter_values})
            if param_unit is not None:
                parameters.update({f"{param_key}_unit": param_unit})

    visual_stimulation = VisualStimulation(
                            stimulus_name=table_key,
                            stimulus_parameters=parameters,
                            stimulus_template_name=intervals_table['stimulus_name'].unique().tolist(),
                            notes=None,
                        )
    return visual_stimulation

def convert_intervals_to_stimulus_epochs(stimulus_name: str, table_key: str, intervals_table: pd.DataFrame) -> StimulusEpoch:
    return StimulusEpoch(
                stimulus_start_time=timedelta(seconds=intervals_table['start_time'].values[0]) + nwbfile.session_start_time,
                stimulus_end_time=timedelta(seconds=intervals_table['stop_time'].values[-1]) + nwbfile.session_start_time,
                stimulus_name=stimulus_name,
                # TODO - acquire additional info about the code used for this task - might not be available 
                # will need to fill in with some type of information so we can use the Code.parameters field @Saskia
                code=Code(
                    url="None",
                    core_dependency=Software(
                        name="PsychoPy",
                        version=None,
                    ), # TODO - from whitepaper, add version if available @Saskia
                    parameters={table_key: get_visual_stimulation_parameters(table_key, intervals_table)},
                ),
                stimulus_modalities=[StimulusModality.VISUAL],
                notes=None,
                active_devices=["None"],
            )

def get_stimulation_epochs(nwbfile):
    # loop through all intervals tables
    stimulation_epochs = []
    for table_key, intervals_table in nwbfile.intervals.items():
        # skip generic trials table that contains behavioral data and invalid_times sections
        if table_key in ["trials", "invalid_times"]:
            continue

        # Convert table key to formatted timulus name
        stimulus_name = table_key.replace('_', ' ').title()

        intervals_table_filtered = intervals_table.to_dataframe()
        stim_epoch = convert_intervals_to_stimulus_epochs(
            stimulus_name=stimulus_name,
            table_key=table_key,
            intervals_table=intervals_table_filtered
        )
        stimulation_epochs.append(stim_epoch)
    
    if 'optotagging' in nwbfile.processing:
        optogenetic_stimulation = nwbfile.processing['optotagging']['optogenetic_stimulation']
        opto_stim_epoch = StimulusEpoch(
            stimulus_start_time=timedelta(seconds=optogenetic_stimulation['start_time'][0]) + nwbfile.session_start_time,
            stimulus_end_time=timedelta(seconds=optogenetic_stimulation['stop_time'][-1]) + nwbfile.session_start_time,
            stimulus_name="Optotagging",
            code=Code( # TODO - add code source if available
                url="None",
                parameters=get_optostimulation_parameters(optogenetic_stimulation),
            ),
            stimulus_modalities=[StimulusModality.OPTOGENETICS],
            performance_metrics=None,
            notes=None,
            # TODO - there was also a 465nm LED option in this dataset, need to determine which was used for which session
            active_devices=["Laser_1"],
            configurations=[LaserConfig(
                    device_name="Laser_1",
                    wavelength=473, # from technical whitepaper
                    wavelength_unit=SizeUnit.NM,
                ),
            ],
            training_protocol_name=None,
            curriculum_status=None,
        )
        stimulation_epochs.append(opto_stim_epoch)

    return stimulation_epochs

acquisition = Acquisition(
    subject_id=nwbfile.subject.subject_id,
    acquisition_start_time=nwbfile.session_start_time,
    acquisition_end_time=get_data_stream_end_time(nwbfile),
    ethics_review_id=None, #TODO - obtain if available - YES, @Saskia
    instrument_id=next(iter(nwbfile.devices)),
    acquisition_type=nwbfile.stimulus_notes, # TODO - assert correct field for this data and present in both functional connectivity and brain obeservatory datasets
    notes=None,
    coordinate_system=CoordinateSystemLibrary.BREGMA_ARID, # TODO - determine correct coordinate system library, will also be defined with instrument (not required to be same as acquisition)
    # coordinate system info might not be available, will check @Saskia
    # calibrations=[], # TODO - add if available - will be difficult to find, probably not
    # maintenance=[],
    data_streams=[
        DataStream(
            stream_start_time=get_data_stream_start_time(nwbfile),
            stream_end_time=get_data_stream_end_time(nwbfile),
            modalities=get_modalities(nwbfile),
            code=None,
            notes=None,
            # active devices will be placeholders depending on the instrument information getting filled in
            # configurations will also be dependent on instrument information 
            # TODO - wait for instrument information but could maybe get some placeholders for active device names @Saskia
            active_devices=[
                "EPHYS_1", # TODO - add conditional for behavioral data to select appropriate devices
                "Laser_1",
                "Lick_Spout_1",
            ],
            configurations=[
                EphysAssemblyConfig(
                    device_name="EPHYS_1",
                    manipulator=ManipulatorConfig(
                        device_name="Manipulator_1", # TODO - fill in with correct information
                        coordinate_system=CoordinateSystemLibrary.MPM_MANIP_RFB, # should be standardized (confirm relative to bregma, positions) @Saskia
                        local_axis_positions=Translation(translation=[0, 0, 0],), # TODO - fill in with correct positions @Saskia
                    ),
                    probes=get_probe_configs(nwbfile),
                ),
                # TODO - there was also a 465nm LED stimulation option in this dataset, need to determine which was used for which session
                LaserConfig( # TODO - should this go here or in the stimulation epochs configuration field?
                    device_name="Laser_1", # placeholder
                    wavelength=473, # from technical whitepaper
                    wavelength_unit=SizeUnit.NM,
                ),
                # TODO - confirm that no lick spout / reward was included in these experiments
            ],
         ),
    ],
    stimulus_epochs=get_stimulation_epochs(nwbfile),
    subject_details=AcquisitionSubjectDetails(
        animal_weight_prior=None, # TODO - pull in extra info if available - likely not available @Saskia
        animal_weight_post=None,
        weight_unit=MassUnit.G,
        mouse_platform_name="Running Wheel",
    ),
)


if __name__ == "__main__":
    serialized = acquisition.model_dump_json()
    deserialized = Acquisition.model_validate_json(serialized)
    deserialized.write_standard_file(prefix=repo_root / f"data/schema/ephys_visual_coding_sub-{subject_id}_ses-{session_id}")
