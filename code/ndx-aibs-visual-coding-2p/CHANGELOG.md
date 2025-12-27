# Changelog

## Version 0.1.0 (December 2025)

Initial release of ndx-aibs-visual-coding-2p extension.

### New Features
- Added `OphysExperimentMetadata` neurodata type that extends `LabMetaData`
- Stores experiment metadata for the session from the original Allen Brain Observatory Visual Coding 2p dataset on S3 (`s3://allen-brain-observatory/visual-coding-2p/ophys_experiments.json`) as a JSON string
- Includes metadata that was not present in the original NWB 1.0 or updated NWB 2.0 files
