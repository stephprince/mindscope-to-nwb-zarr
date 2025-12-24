import json
import pandas as pd

from pathlib import Path

from allensdk.brain_observatory.behavior.behavior_project_cache import VisualBehaviorNeuropixelsProjectCache
from allensdk.brain_observatory.behavior.behavior_project_cache import VisualBehaviorOphysProjectCache
from allensdk.brain_observatory.ecephys.ecephys_project_cache import EcephysProjectCache
from allensdk.core.brain_observatory_cache import BrainObservatoryCache

from allensdk.brain_observatory.ecephys.ecephys_project_api.utilities import build_and_execute, rma_macros

mouse_ids = dict()

# download Visual Behavior - Neuropixels data to local cache
output_dir =  Path(".cache/visual_behavior_neuropixels_cache_dir")
visual_behavior_neuropixels_cache = VisualBehaviorNeuropixelsProjectCache.from_s3_cache(cache_dir=output_dir)

vis_behavior_neuropixels_ephys_session_table = visual_behavior_neuropixels_cache.get_ecephys_session_table()
vis_behavior_neuropixels_session_table = visual_behavior_neuropixels_cache.get_behavior_session_table()

mouse_ids_vis_behavior_neuropixels = vis_behavior_neuropixels_session_table['mouse_id'].unique().tolist()
mouse_ids_vis_behavior_neuropixels_ephys = vis_behavior_neuropixels_ephys_session_table['mouse_id'].unique().tolist()
mouse_ids['visual_behavior_ephys'] = list(set(mouse_ids_vis_behavior_neuropixels) | set(mouse_ids_vis_behavior_neuropixels_ephys))

# dwnload Visual Behavior - Optophysiology data to local cache
output_dir =  Path(".cache/visual_behavior_ophys_cache_dir")
visual_behavior_ophys_cache = VisualBehaviorOphysProjectCache.from_s3_cache(cache_dir=output_dir)

vis_behavior_ophys_session_table = visual_behavior_ophys_cache.get_ophys_session_table()
behavior_session_table = visual_behavior_ophys_cache.get_behavior_session_table()

# pull all unique mouse ids from the behavior sessions file
mouse_ids_ophys_behavior = behavior_session_table['mouse_id'].unique().astype(int).tolist()
mouse_ids_ophys = behavior_session_table['mouse_id'].unique().astype(int).tolist()
mouse_ids['visual_behavior_ophys'] = list(set(mouse_ids_ophys_behavior) | set(mouse_ids_ophys))

# Download Visual Coding - Neuropixels data to local cache
output_dir =  Path(".cache/visual_coding_ephys_cache_dir")
visual_coding_ephys_cache = EcephysProjectCache.from_warehouse(manifest=str(Path(output_dir) / 'visual_coding_ephys_manifest.json'))
coding_ephys_sessions = visual_coding_ephys_cache.get_session_table()

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
    engine=visual_coding_ephys_cache.fetch_api.rma_engine.get_rma_tabular,
    session_ids=None,
)
# donor_ids = set([s['donor_id'] for s in full_session_table['specimen'].tolist()])  # These match subject IDs on DANDI but are not used
external_specimen_ids = set([int(s['external_specimen_name']) for s in full_session_table['specimen'].tolist()])
mouse_ids['visual_coding_ephys'] = external_specimen_ids

# Download Visual Coding - Optophysiology data to local cache
output_dir =  Path(".cache/visual_coding_ophys_cache_dir")
visual_coding_ophys_cache = BrainObservatoryCache(manifest_file=str(Path(output_dir) / 'visual_coding_ophys_manifest.json'))
sessions_df = pd.DataFrame(visual_coding_ophys_cache.get_ophys_experiments(include_failed=True))
sessions_df = sessions_df.query('id <= 717913184') # no session ids greater than this on AWS bucket / DANDI

donor_name_ids = sessions_df['donor_name'].unique().astype(int).tolist()
mouse_ids['visual_coding_ophys'] = donor_name_ids

# save output data
mouse_ids_serializable = {k: sorted(list(v)) if isinstance(v, set) else sorted(v)
                          for k, v in mouse_ids.items()}
with open(Path("data/mouse_ids_by_experiment.json"), 'w') as f:
    json.dump(mouse_ids_serializable, f, indent=2)