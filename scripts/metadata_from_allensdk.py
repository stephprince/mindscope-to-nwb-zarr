import allensdk

from allensdk.brain_observatory.behavior.behavior_project_cache import VisualBehaviorOphysProjectCache
from pathlib import Path

output_dir = ".cache/visual_behavior_ophys_cache_dir"
output_dir = Path(output_dir)
cache = VisualBehaviorOphysProjectCache.from_s3_cache(cache_dir=output_dir)

cache.list_manifest_file_names()

behavior_sessions = cache.get_behavior_session_table() 
print(behavior_sessions)