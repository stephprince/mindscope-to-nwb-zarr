from pathlib import Path

from allensdk.brain_observatory.behavior.behavior_project_cache import VisualBehaviorNeuropixelsProjectCache
from allensdk.brain_observatory.behavior.behavior_project_cache import VisualBehaviorOphysProjectCache
from allensdk.brain_observatory.ecephys.ecephys_project_cache import EcephysProjectCache
from allensdk.core.brain_observatory_cache import BrainObservatoryCache

# download Visual Behavior - Neuropixels data to local cache
# instructions from https://allensdk.readthedocs.io/en/latest/_static/examples/nb/visual_behavior_neuropixels_data_access.html

output_dir =  Path(".cache/visual_behavior_neuropixels_cache_dir")
cache = VisualBehaviorNeuropixelsProjectCache.from_s3_cache(cache_dir=output_dir)
cache.list_manifest_file_names()

ephys_session_table = cache.get_ecephys_session_table()
behavior_session_table = cache.get_behavior_session_table()
probe_table = cache.get_probe_table()
unit_table = cache.get_unit_table()
channel_table = cache.get_channel_table()

# Download Visual Behavior - Optophysiology data to local cache
# instructions from https://allensdk.readthedocs.io/en/latest/_static/examples/nb/visual_behavior_ophys_data_access.html
output_dir =  Path(".cache/visual_behavior_ophys_cache_dir")
cache = VisualBehaviorOphysProjectCache.from_s3_cache(cache_dir=output_dir)
cache.list_manifest_file_names()

ephys_session_table = cache.get_ophys_session_table()
behavior_session_table = cache.get_behavior_session_table()
experiment_table = cache.get_ophys_experiment_table()

# Download Visual Coding - Neuropixels data to local cache
# instructions from https://allensdk.readthedocs.io/en/latest/_static/examples/nb/ecephys_data_access.html
output_dir =  Path(".cache/visual_coding_ephys_cache_dir")
cache = EcephysProjectCache.from_warehouse(manifest=str(Path(output_dir) / 'visual_coding_ephys_manifest.json'))
coding_ephys_sessions = cache.get_session_table()
probe_table = cache.get_probes()
unit_table = cache.get_units()
channel_table = cache.get_channels()

# Download Visual Coding - Optophysiology data to local cache
# instructions from https://allensdk.readthedocs.io/en/latest/_static/examples/nb/brain_observatory.html
output_dir =  Path(".cache/visual_coding_ophys_cache_dir")
cache = BrainObservatoryCache(manifest_file=str(Path(output_dir) / 'visual_coding_ophys_manifest.json'))
experiment_info = cache.get_experiment_containers()
sessions = cache.get_ophys_experiments()
