"""
Convert NWB cached specification files to standard JSON files.

This script reads the specifications group from an NWB file (either HDF5 or Zarr),
iterates over each spec namespace (e.g., core, hdmf-common, ndx-aibs-ecephys), and
for each namespace and version, converts the cached specs to proper JSON files.

Output files are named with pattern: {namespace}_{version}_{spec_file}.json, e.g.:
core_2.2.2_nwb.base.json

By default, output is saved to data/exported_specs/{input_filename_with_periods_as_underscores}/

Example usage:
    python scripts/nwb_cached_specs_to_json.py path/to/file.nwb
    python scripts/nwb_cached_specs_to_json.py path/to/file.nwb.zarr
    python scripts/nwb_cached_specs_to_json.py path/to/file.nwb -o custom_output_directory
"""

import json
import argparse
from pathlib import Path
import zarr
import h5py
import shutil


def extract_specs_from_zarr(zarr_path: str, output_dir: str):
    """
    Extract all specification files from an NWB Zarr store and save as JSON files.

    Args:
        zarr_path: Path to the NWB Zarr store
        output_dir: Directory to write the JSON files
    """
    # Open the Zarr store
    store = zarr.open(zarr_path, mode='r')

    # Check if specifications group exists
    if 'specifications' not in store:
        raise ValueError(f"No 'specifications' group found in {zarr_path}")

    specs_group = store['specifications']

    # Clear output directory if it exists, then create it
    output_path = Path(output_dir)
    if output_path.exists():
        shutil.rmtree(output_path)
        print(f"Removed existing directory: {output_path}")
    output_path.mkdir(parents=True, exist_ok=True)

    # Track statistics
    total_specs = 0

    # Iterate over each namespace
    namespaces = list(specs_group.keys())
    print(f"Found {len(namespaces)} namespaces: {namespaces}")
    print()

    for namespace in namespaces:
        namespace_group = specs_group[namespace]

        # Iterate over each version in the namespace
        versions = list(namespace_group.keys())
        print(f"Namespace '{namespace}' has {len(versions)} version(s): {versions}")

        for version in versions:
            version_group = namespace_group[version]

            # Iterate over each spec file in the version
            spec_files = list(version_group.keys())
            print(f"  Version '{version}' has {len(spec_files)} spec file(s): {spec_files}")

            for spec_file in spec_files:
                # Read the spec data (should be a 1-element array)
                spec_array = version_group[spec_file]
                spec_data = spec_array[:]

                # Verify it's a single chunk
                assert len(spec_data) == 1, f"Expected 1 chunk, got {len(spec_data)}"

                # Extract the JSON data (stored as a JSON string)
                json_string = spec_data[0]

                # Parse the JSON string to get the actual data
                json_data = json.loads(json_string)

                # Create output filename
                output_filename = f"{namespace}_{version}_{spec_file}.json"
                output_filepath = output_path / output_filename

                # Write the JSON file
                with open(output_filepath, 'w') as fout:
                    json.dump(json_data, fout, indent=2)

                print(f"    Wrote: {output_filename}")
                total_specs += 1

            print()

    print(f"Successfully extracted {total_specs} specification files to {output_path}")


def extract_specs_from_hdf5(hdf5_path: str, output_dir: str):
    """
    Extract all specification files from an NWB HDF5 file and save as JSON files.

    Args:
        hdf5_path: Path to the NWB HDF5 file
        output_dir: Directory to write the JSON files
    """
    # Open the HDF5 file
    with h5py.File(hdf5_path, 'r') as f:
        # Check if specifications group exists
        if 'specifications' not in f:
            raise ValueError(f"No 'specifications' group found in {hdf5_path}")

        specs_group = f['specifications']

        # Clear output directory if it exists, then create it
        output_path = Path(output_dir)
        if output_path.exists():
            shutil.rmtree(output_path)
            print(f"Removed existing directory: {output_path}")
        output_path.mkdir(parents=True, exist_ok=True)

        # Track statistics
        total_specs = 0

        # Iterate over each namespace
        namespaces = list(specs_group.keys())
        print(f"Found {len(namespaces)} namespaces: {namespaces}")
        print()

        for namespace in namespaces:
            namespace_group = specs_group[namespace]

            # Iterate over each version in the namespace
            versions = list(namespace_group.keys())
            print(f"Namespace '{namespace}' has {len(versions)} version(s): {versions}")

            for version in versions:
                version_group = namespace_group[version]

                # Iterate over each spec file in the version
                spec_files = list(version_group.keys())
                print(f"  Version '{version}' has {len(spec_files)} spec file(s): {spec_files}")

                for spec_file in spec_files:
                    # Read the spec data (stored as a scalar dataset containing JSON bytes)
                    spec_dataset = version_group[spec_file]
                    spec_data = spec_dataset[()]

                    # Convert bytes to string if necessary
                    if isinstance(spec_data, bytes):
                        json_string = spec_data.decode('utf-8')
                    else:
                        json_string = spec_data

                    # Parse the JSON string to get the actual data
                    json_data = json.loads(json_string)

                    # Create output filename
                    output_filename = f"{namespace}_{version}_{spec_file}.json"
                    output_filepath = output_path / output_filename

                    # Write the JSON file
                    with open(output_filepath, 'w') as fout:
                        json.dump(json_data, fout, indent=2)

                    print(f"    Wrote: {output_filename}")
                    total_specs += 1

                print()

        print(f"Successfully extracted {total_specs} specification files to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Extract NWB cached specifications from HDF5 or Zarr files to JSON files"
    )
    parser.add_argument(
        'nwb_path',
        type=str,
        help='Path to the NWB file (HDF5 or Zarr store)'
    )
    parser.add_argument(
        '-o', '--output-dir',
        type=str,
        default=None,
        help='Output directory for JSON files (default: data/exported_specs/{input_filename_with_underscores})'
    )

    args = parser.parse_args()

    # Determine file type based on extension or directory
    nwb_path = Path(args.nwb_path)

    # Generate default output directory if not specified
    if args.output_dir is None:
        # Get the filename (with extension) and replace periods with underscores
        input_filename = nwb_path.name
        sanitized_filename = input_filename.replace('.', '_')
        args.output_dir = f'data/exported_specs/{sanitized_filename}'

    if nwb_path.is_dir() or str(nwb_path).endswith('.zarr'):
        print(f"Detected Zarr store: {nwb_path}")
        extract_specs_from_zarr(str(nwb_path), args.output_dir)
    elif nwb_path.is_file() and str(nwb_path).endswith('.nwb'):
        print(f"Detected HDF5 file: {nwb_path}")
        extract_specs_from_hdf5(str(nwb_path), args.output_dir)
    else:
        raise ValueError(
            f"Could not determine file type for: {nwb_path}. "
            f"Expected either a .nwb file (HDF5) or .zarr directory/store."
        )


if __name__ == '__main__':
    main()
