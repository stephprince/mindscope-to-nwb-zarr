"""Generates Vis Coding 2P Instrument"""

from datetime import date

from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.units import FrequencyUnit, SizeUnit
from aind_data_schema_models.devices import CameraTarget, LickSensorType
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
    DataInterface,
    Detector,
    Device,
    Disc,
    Filter,
    FilterType,
    Laser,
    LickSpout,
    Lens,
    Microscope,
    Monitor,
    Objective,
)

from aind_data_schema.core.instrument import Instrument

instrument = Instrument(
    location="Unknown",
    instrument_id="CAM2P.3",
    modification_date=date(2016, 10, 12),
    coordinate_system=CoordinateSystemLibrary.BREGMA_ARI,
    modalities=[Modality.POPHYS, Modality.BEHAVIOR_VIDEOS],
    notes="Created several years posthoc from incomplete records. Much information is missing.",
    temperature_control=None,
    components=[
        Microscope(
            name="Scientifica",
            manufacturer=Organization.SCIENTIFICA,
            model="Vivoscope",
        ),
        Laser(
            name="Ti-Saph",
            wavelength=910,
            wavelength_unit=SizeUnit.NM,
            manufacturer=Organization.COHERENT_SCIENTIFIC,
            model="Chameleon Vision",
        ),
        Objective(
            name="Nikon 16x",
            numerical_aperture=0.8,
            magnification=16,
            manufacturer=Organization.NIKON,
            immersion="water",
            model="N16XLWD-PF",
        ),
        Detector(
            name="PMT",
            manufacturer=Organization.UNKNOWN,
            detector_type="Photomultiplier Tube",
            data_interface=DataInterface.OTHER,
            notes="Unknown data interface"
        ),
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
            transform=[Rotation(angles=[45, 90, 0]),Translation(translation=[86.2, 118.6, 31.6])]
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
        LickSpout(
            name="Reward spout",
            manufacturer=Organization.HAMILTON,
            model="8649-01",
            spout_diameter=0.672,
            solenoid_valve=Device(
                name="Solenoid valve",
                manufacturer=Organization.NRESEARCH_INC,
                model="161K011",
            ),
            lick_sensor=Device(
                name="Lick sensor",
                manufacturer=Organization.TE_CONNECTIVITY,
                model="1007079-1",
            ),
            lick_sensor_type=LickSensorType.PIEZOELECTIC,
        ),
        
    ]
)

if __name__ == "__main__":
    serialized = instrument.model_dump_json()
    deserialized = Instrument.model_validate_json(serialized)
    deserialized.write_standard_file(prefix="cam2p3_final_")