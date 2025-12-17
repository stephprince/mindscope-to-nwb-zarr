import json

from pathlib import Path

from allensdk.brain_observatory.behavior.behavior_project_cache import VisualBehaviorNeuropixelsProjectCache
from allensdk.brain_observatory.behavior.behavior_project_cache import VisualBehaviorOphysProjectCache
from allensdk.brain_observatory.ecephys.ecephys_project_cache import EcephysProjectCache
from allensdk.core.brain_observatory_cache import BrainObservatoryCache

from allensdk.brain_observatory.ecephys.ecephys_project_api.utilities import build_and_execute, rma_macros

mouse_ids = dict()

# download Visual Behavior - Neuropixels data to local cache
output_dir =  Path(".cache/visual_behavior_neuropixels_cache_dir")
cache = VisualBehaviorNeuropixelsProjectCache.from_s3_cache(cache_dir=output_dir)

ephys_session_table = cache.get_ecephys_session_table()
behavior_session_table = cache.get_behavior_session_table()

mouse_ids_behavior = behavior_session_table['mouse_id'].unique().tolist()
mouse_ids_ephys = ephys_session_table['mouse_id'].unique().tolist()
mouse_ids['visual_behavior_ephys'] = list(set(mouse_ids_behavior) | set(mouse_ids_ephys))

# dwnload Visual Behavior - Optophysiology data to local cache
output_dir =  Path(".cache/visual_behavior_ophys_cache_dir")
cache = VisualBehaviorOphysProjectCache.from_s3_cache(cache_dir=output_dir)

ephys_session_table = cache.get_ophys_session_table()
behavior_session_table = cache.get_behavior_session_table()

# pull all unique mouse ids from the behavior sessions file
mouse_ids_behavior = behavior_session_table['mouse_id'].unique().astype(int).tolist()
mouse_ids_ephys = ephys_session_table['mouse_id'].unique().astype(int).tolist()
mouse_ids['visual_behavior_ophys'] = list(set(mouse_ids_behavior) | set(mouse_ids_ephys))

# Download Visual Coding - Neuropixels data to local cache
output_dir =  Path(".cache/visual_coding_ephys_cache_dir")
cache = EcephysProjectCache.from_warehouse(manifest=str(Path(output_dir) / 'visual_coding_ephys_manifest.json'))
coding_ephys_sessions = cache.get_session_table()

# pull all unique mouse ids from the behavior sessions file
full_session_table = build_and_execute(
    (
        "{% import 'rma_macros' as rm %}"
        "{% import 'macros' as m %}"
        "criteria=model::EcephysSession"
        r"{{rm.optional_contains('id',session_ids)}}"
        ",rma::include,specimen(donor(age))"
    ),
    base=rma_macros(),
    engine=cache.fetch_api.rma_engine.get_rma_tabular,
    session_ids=None,
)
donor_ids = set([s['donor_id'] for s in full_session_table['specimen'].tolist()])  # These match subject IDs on DANDI
specimen_ids = coding_ephys_sessions['specimen_id'].unique().tolist() 
external_specimen_ids = set([s['external_specimen_name'] for s in full_session_table['specimen'].tolist()])  # These match subject IDs on DANDI
mouse_ids['visual_coding_ephys'] = external_specimen_ids
# TODO - external_specimen_name here seems to be 6 digits

# Download Visual Coding - Optophysiology data to local cache
output_dir =  Path(".cache/visual_coding_ophys_cache_dir")
cache = BrainObservatoryCache(manifest_file=str(Path(output_dir) / 'visual_coding_ophys_manifest.json'))
sessions = cache.get_ophys_experiments()
experiment_info = cache.get_experiment_containers()
with open(Path(output_dir) / 'experiment_containers.json', 'r') as f:
    experiment_containers = json.load(f)

specimen_ids = set(list({e["specimen_id"] for e in experiment_containers})) # These match the subject IDs on DANDI
donor_ids = set(list({e["specimen"]["donor_id"] for e in experiment_containers})) 
specimen_ids_filtered = [s for s in specimen_ids if s <= 699502603] # dandiset and S3 bucket only includes data from these mouse ids
donor_name_ids = set([s["donor_name"] for s in sessions])
mouse_ids['visual_coding_ophys'] = donor_name_ids
# TODO - donor name here seems to be correct

# save output data
mouse_ids_serializable = {k: sorted(list(v)) if isinstance(v, set) else sorted(v)
                          for k, v in mouse_ids.items()}
with open(Path("data/mouse_ids_by_experiment.json"), 'w') as f:
    json.dump(mouse_ids_serializable, f, indent=2)