import allensdk

from allensdk.brain_observatory.behavior.behavior_project_cache import VisualBehaviorNeuropixelsProjectCache
from allensdk.brain_observatory.behavior.behavior_project_cache import VisualBehaviorOphysProjectCache
from pathlib import Path

# download visual behavior neuropixels data to local cache
# instructions from https://allensdk.readthedocs.io/en/latest/_static/examples/nb/visual_behavior_neuropixels_data_access.html

output_dir =  Path(".cache/visual_behavior_neuropixels_cache_dir")
cache = VisualBehaviorNeuropixelsProjectCache.from_s3_cache(cache_dir=output_dir)
cache.list_manifest_file_names()

ephys_session_table = cache.get_ecephys_session_table()
behavior_session_table = cache.get_behavior_session_table()
probe_table = cache.get_probe_table()
unit_table = cache.get_unit_table()
channel_table = cache.get_channel_table()

# download visual behavior optophysiology data to local cache
# instructions from https://allensdk.readthedocs.io/en/latest/_static/examples/nb/visual_behavior_ophys_data_access.html
output_dir =  Path(".cache/visual_behavior_ophys_cache_dir")
cache = VisualBehaviorOphysProjectCache.from_s3_cache(cache_dir=output_dir)
cache.list_manifest_file_names()

ephys_session_table = cache.get_ophys_session_table()
behavior_session_table = cache.get_behavior_session_table()
experiment_table = cache.get_ophys_experiment_table()