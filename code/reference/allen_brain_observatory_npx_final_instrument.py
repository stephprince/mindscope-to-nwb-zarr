"""Generates Allen Brain Observatory Neuropixels Instrument"""

from datetime import date

from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.units import FrequencyUnit, SizeUnit
from aind_data_schema_models.devices import CameraTarget
from aind_data_schema_models.coordinates import AnatomicalRelative

from aind_data_schema.components.coordinates import (
    CoordinateSystemLibrary,
    Rotation,
    Translation,
)
from aind_data_schema.components.devices import (
    Camera,
    CameraAssembly,
    Cooling,
    Disc,
    EphysAssembly,
    EphysProbe,
    Filter,
    FilterType,
    Laser,
    Lens,
    Manipulator,
    Monitor,
    ProbeModel,
)
from aind_data_schema.components.identifiers import Software

from aind_data_schema.core.instrument import Instrument

# Generate EphysAssembly objects A-F
ephys_assemblies = [
    EphysAssembly(
        name=f"Ephys Assembly {letter}",
        manipulator=Manipulator(
            name=f"Ephys Assembly {letter} Manipulator",
            manufacturer=Organization.NEW_SCALE_TECHNOLOGIES,
            model="M3-LS-1.8-6",
        ),
        probes=[
            EphysProbe(
                name=f"Probe{letter}",
                manufacturer=Organization.IMEC,
                probe_model=ProbeModel.NP1,
            )
        ],
    )
    for letter in "ABCDEF"
]

instrument = Instrument(
    location="325",
    instrument_id="NP.X",
    modification_date=date(2019, 10, 11),
    coordinate_system=CoordinateSystemLibrary.BREGMA_ARI,
    modalities=[Modality.ECEPHYS, Modality.BEHAVIOR_VIDEOS],
    notes="Created several years posthoc from incomplete records. Much information is missing.",
    temperature_control=None,
    components=[
        Monitor(
            name="Stimulus Screen",
            serial_number=None,
            manufacturer=Organization.ASUS,
            model="PA248Q",
            notes="viewing distance is from screen normal to bregma",
            refresh_rate=60,
            width=1920,
            height=1200,
            size_unit="pixel",
            viewing_distance=17,
            viewing_distance_unit="centimeter",
            relative_position=[AnatomicalRelative.ANTERIOR, AnatomicalRelative.RIGHT],
            contrast=30,
            brightness=50,
            coordinate_system=CoordinateSystemLibrary.SIPE_MONITOR_RTF,
            transform=[
                Rotation(angles=[45, 90, 0]),
                Translation(translation=[86.2, 118.6, 31.6]),
            ],
        ),
        Disc(
            name="MindScope Running Disc",
            manufacturer=Organization.AIND,
            radius=8.255,
            radius_unit="centimeter",
        ),
        CameraAssembly(
            name="Eye Camera Assembly",
            target=CameraTarget.EYE,
            relative_position=[AnatomicalRelative.RIGHT],
            camera=Camera(
                name="Eye Camera",
                detector_type="Camera",
                manufacturer=Organization.ALLIED,
                model="Mako G-032B",
                data_interface="Ethernet",
                cooling=Cooling.NO_COOLING,
                frame_rate=30.0,
                frame_rate_unit=FrequencyUnit.HZ,
                chroma="Monochrome",
            ),
            lens=Lens(
                name="Eye Camera Lens",
                manufacturer=Organization.INFINITY_PHOTO_OPTICAL,
                model="InfiniStix",
            ),
            filter=Filter(
                name="Eye Camera Filter",
                filter_type=FilterType.BANDPASS,
                manufacturer=Organization.SEMROCK,
                model="FF01-850/10-25",
                center_wavelength=850,
                wavelength_unit=SizeUnit.NM,
            ),
        ),
        Filter(
            name="Eye Camera Dichroic",
            filter_type=FilterType.DICHROIC,
            manufacturer=Organization.SEMROCK,
            model="FF750-SDi02-25x36",
        ),
        CameraAssembly(
            name="Body Camera Assembly",
            target=CameraTarget.BODY,
            relative_position=[AnatomicalRelative.LEFT, AnatomicalRelative.POSTERIOR],
            camera=Camera(
                name="Body Camera",
                detector_type="Camera",
                manufacturer=Organization.ALLIED,
                model="Mako G-032B",
                data_interface="Ethernet",
                cooling=Cooling.NO_COOLING,
                frame_rate=30.0,
                frame_rate_unit=FrequencyUnit.HZ,
                chroma="Monochrome",
            ),
            lens=Lens(
                name="Body Camera Lens",
                manufacturer=Organization.THORLABS,
                model="MVL8M23",
            ),
            filter=Filter(
                name="Body Camera Filter",
                manufacturer=Organization.SEMROCK,
                model="BSP01-785R-25",
                filter_type=FilterType.SHORTPASS,
                cut_off_wavelength=785,
                wavelength_unit=SizeUnit.NM,
            ),
        ),
        Laser(
            name="Optotagging Laser",
            manufacturer=Organization.UNKNOWN,
            wavelength=473,
            wavelength_unit=SizeUnit.NM,
        ),
        *ephys_assemblies,
    ],
)

if __name__ == "__main__":
    serialized = instrument.model_dump_json()
    deserialized = Instrument.model_validate_json(serialized)
    deserialized.write_standard_file(prefix="allen_brain_observatory_npx")
