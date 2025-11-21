# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Project Overview

This project converts Allen Brain Observatory neurophysiology data from the Allen Institute MindScope program to the NWB (Neurodata Without Borders) format using the Zarr storage backend.

## Development Setup

This project uses `uv` for dependency management. Requires Python 3.11+.

```bash
# Install dependencies
uv sync

# Run Python scripts
uv run python <script.py>
```

## Key Dependencies

- **pynwb**: Core library for reading/writing NWB files
- **hdmf-zarr**: Enables Zarr backend storage for NWB (instead of HDF5)
- **aind-data-schema**: Allen Institute for Neural Dynamics data schemas
- **allensdk**: Allen Institute SDK for accessing MindScope data
