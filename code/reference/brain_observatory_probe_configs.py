from aind_data_schema_models.coordinates import AxisName, Direction, Origin
from aind_data_schema_models.units import AngleUnit, SizeUnit
from aind_data_schema_models.brain_atlas import CCFv3

from aind_data_schema.components.coordinates import CoordinateSystem, Axis, Rotation, Translation
from aind_data_schema.components.configs import ProbeConfig

probe_rotations = {
    'A': [-18.1947618, 18.45024358, -16.25274646],
    'B': [  0,         -39.08251859, -19.99997558],
    'C': [ 112.8691441,   -71.24837276, -124.32351133],
    'D': [ 161.8052382,   -18.45024358, -163.74725354],
    'E': [ 180,          39.08251859, -160.00002442],
    'F': [-67.1308559,   71.24837276, -55.67648867]}

probe_offsets = {'A' : [0.770, -0.150, -1.335, 0],
                    'B' : [1.54, -0.149, -0.0004, 0],
                    'C' : [0.770, -0.149, 1.334, 0],
                    'D' : [-0.770, -0.1449, 1.334, 0],
                    'E' : [-1.541, -0.149, 0.0004, 0],
                    'F' : [-0.77094, -0.14997, -1.33436, 0]}

targeted_structures = {'A' : CCFv3.by_acronym('VISam'),
                       'B' : CCFv3.by_acronym('VISpm'),
                       'C' : CCFv3.by_acronym('VISp'),
                       'D' : CCFv3.by_acronym('VISl'),
                       'E' : CCFv3.by_acronym('VISal'),
                       'F' : CCFv3.by_acronym('VISrl')}

reticle_rotation = [ 0., -23.11637603,  0.]
reticle_offset = [-2.777, -3.071, 0.607, 0]

probe_insertion_depth = [0, -3.5, 0, 3.5]

global_coordinate_system = CoordinateSystem(
   name = "BREGMA_RAS",
   origin = Origin.BREGMA,
   axis_unit=SizeUnit.MM,
   axes = [
       Axis(name = AxisName.ML, direction=Direction.LR),
       Axis(name = AxisName.AP, direction=Direction.PA),
       Axis(name = AxisName.SI, direction=Direction.IS)
   ]
)

probe_coordinate_system = CoordinateSystem(
    name = "PROBE_RUFD",
    origin=Origin.TIP,
    axis_unit=SizeUnit.UM, # standard unit for probe coordinates
    axes = [
        Axis(name = AxisName.X, direction=Direction.LR),
        Axis(name = AxisName.Y, direction=Direction.DU),
        Axis(name = AxisName.Z, direction=Direction.BF),
        Axis(name = AxisName.DEPTH, direction=Direction.UD)
    ]
)

for probe_idx, probe in enumerate(probe_rotations.keys()):

    probe_config = ProbeConfig(
        device_name =  probe,
        primary_targeted_structure = targeted_structures[probe],
        coordinate_system = probe_coordinate_system,
        transform = [
            Translation(translation=probe_insertion_depth), # moves the probe along the insertion axis to the approximate depth inside the brain
            Rotation(angles = probe_rotations[probe], angles_unit=AngleUnit.DEG), # sets probe orientation relative to V1 reticle
            Rotation(angles = [90, 0, 0], angles_unit=AngleUnit.DEG), # flips probe by 90 degrees
            Translation(translation=probe_offsets[probe]), # sets probe location relative to V1 reticle
            Rotation(angles = reticle_rotation, angles_unit=AngleUnit.DEG), # rotates reticle-relative coordinates into bregma-relative coordinates
            Translation(translation=reticle_offset) # translates reticle-relative coordinates into bregma-relative coordinates
        ],
        notes = """Six transformations define the position and orientation of the probe in the global coordinate space:

    Translation 1: moves the probe along the insertion axis to the approximate depth inside the brain
    Rotation 1: sets probe orientation relative to V1 reticle
    Rotation 2: flips probe by 90 degrees
    Translation 2: sets probe location relative to V1 reticle
    Rotation 3: rotates reticle-relative coordinates into bregma-relative coordinates
    Translation 3: translates reticle-relative coordinates into bregma-relative coordinates
    """
    )
