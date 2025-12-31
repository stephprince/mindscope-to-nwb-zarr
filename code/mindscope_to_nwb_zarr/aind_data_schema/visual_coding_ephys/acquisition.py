"""Generates acquisition metadata from NWB files for visual coding ephys sessions"""

import pandas as pd

from datetime import timedelta
from pynwb import NWBFile

from aind_data_schema.components.identifiers import Code
from aind_data_schema.core.acquisition import (
    Acquisition,
    StimulusEpoch,
    DataStream,
    AcquisitionSubjectDetails,
)
from aind_data_schema.components.configs import (
    ManipulatorConfig,
    EphysAssemblyConfig,
    LaserConfig,
)
from aind_data_schema.components.coordinates import Translation, CoordinateSystemLibrary
from aind_data_schema_models.units import SizeUnit, MassUnit
from aind_data_schema_models.stimulus_modality import StimulusModality

from mindscope_to_nwb_zarr.pynwb_utils import (
    get_data_stream_start_time,
    get_data_stream_end_time,
    get_modalities
)
from mindscope_to_nwb_zarr.aind_data_schema.utils import (
    get_subject_id,
    get_session_start_time,
    get_instrument_id,
    get_probe_configs,
    get_optostimulation_parameters,
    convert_intervals_to_stimulus_epochs,
)


def get_stimulation_epochs(nwbfile: NWBFile, session_info: pd.DataFrame) -> list[StimulusEpoch]:
    """
    Extract stimulus epochs from NWB file intervals tables.

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing intervals tables
    session_info : pd.DataFrame
        Session metadata information

    Returns
    -------
    list[StimulusEpoch]
        List of stimulus epochs extracted from the NWB file
    """
    stimulation_epochs = []

    for table_key, intervals_table in nwbfile.intervals.items():
        # skip generic trials table that contains behavioral data and invalid_times sections
        if table_key in ["trials", "invalid_times"]:
            continue

        # Convert table key to formatted stimulus name
        stimulus_name = table_key.replace('_', ' ').title()

        intervals_table_filtered = intervals_table.to_dataframe()
        stim_epoch = convert_intervals_to_stimulus_epochs(
            stimulus_name=stimulus_name,
            table_key=table_key,
            intervals_table=intervals_table_filtered,
            nwbfile=nwbfile,
            session_info=session_info
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


def generate_acquisition(nwbfile: NWBFile, session_info: pd.DataFrame) -> Acquisition:
    """
    Generate an Acquisition model from an NWB file and session metadata.

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing acquisition data
    session_info : pd.DataFrame
        Session metadata information

    Returns
    -------
    Acquisition
        AIND Acquisition data model populated with data from the NWB file
    """
    breakpoint()
    acquisition = Acquisition(
        subject_id=nwbfile.subject.subject_id,
        acquisition_start_time=nwbfile.session_start_time,
        acquisition_end_time=get_data_stream_end_time(nwbfile),
        ethics_review_id=None, #TODO - obtain if available - YES, @Saskia
        instrument_id=next(iter(nwbfile.devices)), # TODO - confirm correct instrument id
        acquisition_type=nwbfile.stimulus_notes, # TODO - assert correct field for this data and present in both functional connectivity and brain observatory datasets
        notes=None,
        coordinate_system=CoordinateSystemLibrary.BREGMA_ARID, # TODO - determine correct coordinate system library, will also be defined with instrument (not required to be same as acquisition)
        # coordinate system info might not be available, will check @Saskia
        # calibrations=[], # TODO - add if available - will be difficult to find, probably not
        # maintenance=[],
        data_streams=[
            DataStream(
                stream_start_time=get_data_stream_start_time(nwbfile),
                stream_end_time=get_data_stream_end_time(nwbfile),
                modalities=get_modalities(nwbfile), # TODO - include ISI data?
                code=None,
                notes=None,
                # active devices will be placeholders depending on the instrument information getting filled in
                # configurations will also be dependent on instrument information
                # TODO - wait for instrument information but could maybe get some placeholders for active device names @Saskia
                active_devices=[
                    "EPHYS_1", # TODO - add conditional for behavioral data to select appropriate devices
                    "Laser_1",
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
                    # TODO - confirm that no lick spout / reward was not included in these experiments
                ],
             ),
        ],
        stimulus_epochs=get_stimulation_epochs(nwbfile, session_info),
        subject_details=AcquisitionSubjectDetails(
            animal_weight_prior=None, # TODO - pull in extra info if available - likely not available @Saskia
            animal_weight_post=None,
            weight_unit=MassUnit.G,
            mouse_platform_name="Running Wheel",
        ),
    )

    return acquisition
