"""Generates Behavior Box Instrument"""

from datetime import date

from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.units import FrequencyUnit, SizeUnit
from aind_data_schema_models.devices import CameraTarget, LickSensorType
from aind_data_schema_models.coordinates import AnatomicalRelative

from aind_data_schema.components.coordinates import (
    CoordinateSystemLibrary,
    Rotation,
    Scale,
    Translation,
)
from aind_data_schema.components.devices import (
    Camera,
    CameraAssembly,
    Cooling,
    Enclosure,
    Device,
    Disc,
    Lens,
    LickSpout,
    Monitor,
)
from aind_data_schema.components.identifiers import Software

from aind_data_schema.core.instrument import Instrument


instrument = Instrument(
    location="Unknown",
    instrument_id="", #TODO names will be e.g. "C2" - letter number
    modification_date=date(2016, 10, 12), #TODO 
    coordinate_system=CoordinateSystemLibrary.BREGMA_ARI,
    modalities=[Modality.BEHAVIOR],
    notes="Created several years posthoc from incomplete records. Much information is missing.",
    temperature_control=None,
    components=[
        Disc(
            name="MindScope Running Disc",
            manufacturer=Organization.AIND,
            radius=8.255,
            radius_unit="centimeter",
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
        Enclosure(
            name="Behavior box enclosure",
            size=Scale(scale=[0.585, 0.435, 0.4]),
            size_unit=SizeUnit.M,
            internal_material="Sound attenuating foam",
            external_material="Aluminium",
            grounded=True,
            laser_interlock=False,
            air_filtration=False,
        ),
        CameraAssembly(
            name="Body Camera Assembly",
            target=CameraTarget.BODY,
            relative_position=[AnatomicalRelative.LEFT],
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
        ),
        CameraAssembly(
            name="Top Camera Assembly",
            target=CameraTarget.BODY,
            relative_position=[AnatomicalRelative.SUPERIOR],
            camera=Camera(
                name="Top Camera",
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
                name="Top Camera Lens",
                manufacturer=Organization.THORLABS,
                model="MVL8M23",
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
        )
    ],
)

if __name__ == "__main__":
    serialized = instrument.model_dump_json()
    deserialized = Instrument.model_validate_json(serialized)
    deserialized.write_standard_file(prefix="behavior_")
