from pynwb import NWBHDF5IO
from hdmf_zarr.nwb import NWBZarrIO
from pynwb.testing import TestCase

filename = "data/sub-506940_ses-20200228T111117_image.nwb"
zarr_filename = "data/sub-506940_ses-20200228T111117_image.nwb.zarr"

from hdmf.container import AbstractContainer, Container, Data, AbstractDataChunkIterator
from hdmf.utils import get_docval_macro
import numpy as np
from warnings import warn

# Based on pynwb.testing.TestCase.assertContainerEqual but raises warnings instead of assertions
class NWBContainerComparator:
    def check_container_equal(self,
                            container1,
                            container2,
                            ignore_name=False,
                            ignore_hdmf_attrs=False,
                            ignore_string_to_byte=False,
                            message=None):
        """
        Asserts that the two AbstractContainers have equal contents. This applies to both Container and Data types.

        :param container1: First container
        :type container1: AbstractContainer
        :param container2: Second container to compare with container 1
        :type container2: AbstractContainer
        :param ignore_name: whether to ignore testing equality of name of the top-level container
        :param ignore_hdmf_attrs: whether to ignore testing equality of HDMF container attributes, such as
                                    container_source and object_id
        :param ignore_string_to_byte: ignore conversion of str to bytes and compare as unicode instead
        :param message: custom additional message to show when assertions as part of this assert are failing
        """
        if not isinstance(container1, AbstractContainer):
            warn(f"{container1} must be an AbstractContainer")
        if not isinstance(container2, AbstractContainer):
            warn(f"{container2} must be an AbstractContainer")
        type1 = type(container1)
        type2 = type(container2)
        if type1 != type2:
            warn(f"Container types do not match: {type1} != {type2}")
        if not ignore_name:
            if container1.name != container2.name:
                warn(f"Container names do not match: {container1.name} != {container2.name}")
        if not ignore_hdmf_attrs:
            if container1.container_source != container2.container_source:
                warn(f"Container sources do not match: {container1.container_source} != {container2.container_source}")
            if container1.object_id != container2.object_id:
                warn(f"Container object IDs do not match: {container1.object_id} != {container2.object_id}")
        # NOTE: parent is not tested because it can lead to infinite loops
        if isinstance(container1, Container):
            if len(container1.children) != len(container2.children):
                warn(f"Number of children do not match: {len(container1.children)} != {len(container2.children)}")
        # do not actually check the children values here. all children *should* also be fields, which is checked below.
        # this is in case non-field children are added to one and not the other

        for field in getattr(container1, type1._fieldsname):
            f1 = getattr(container1, field)
            f2 = getattr(container2, field)
            self._check_field_equal(f1, f2,
                                        ignore_hdmf_attrs=ignore_hdmf_attrs,
                                        ignore_string_to_byte=ignore_string_to_byte,
                                        message=message)

    def _check_field_equal(self,
                        f1,
                        f2,
                        ignore_hdmf_attrs=False,
                        ignore_string_to_byte=False,
                        message=None):
        """
        Internal helper function used to compare two fields from Container objects

        :param f1: The first field
        :param f2: The second field
        :param ignore_hdmf_attrs: whether to ignore testing equality of HDMF container attributes, such as
                                    container_source and object_id
        :param ignore_string_to_byte: ignore conversion of str to bytes and compare as unicode instead
        :param message: custom additional message to show when assertions as part of this assert are failing
        """
        array_data_types = get_docval_macro('array_data')
        if (isinstance(f1, array_data_types) or isinstance(f2, array_data_types)):
            self._check_array_equal(f1, f2,
                                        ignore_hdmf_attrs=ignore_hdmf_attrs,
                                        ignore_string_to_byte=ignore_string_to_byte,
                                        message=message)
        elif isinstance(f1, dict) and len(f1) and isinstance(f1.values()[0], Container):
            if not isinstance(f2, dict):
                warn(f"Type mismatch: expected dict, got {type(f2)}{': ' + message if message else ''}")
            f1_keys = set(f1.keys())
            f2_keys = set(f2.keys())
            if f1_keys != f2_keys:
                warn(f"Dict keys do not match: {f1_keys} != {f2_keys}{': ' + message if message else ''}")
            for k in f1_keys:
                self.check_container_equal(f1[k], f2[k],
                                            ignore_hdmf_attrs=ignore_hdmf_attrs,
                                            ignore_string_to_byte=ignore_string_to_byte,
                                            message=message)
        elif isinstance(f1, Container):
            self.check_container_equal(f1, f2,
                                        ignore_hdmf_attrs=ignore_hdmf_attrs,
                                        ignore_string_to_byte=ignore_string_to_byte,
                                        message=message)
        elif isinstance(f1, Data):
            self._check_data_equal(f1, f2,
                                    ignore_hdmf_attrs=ignore_hdmf_attrs,
                                    ignore_string_to_byte=ignore_string_to_byte,
                                    message=message)
        elif isinstance(f1, (float, np.floating)):
            if not np.allclose(f1, f2, equal_nan=True):
                warn(f"Float values not close: {f1} != {f2}{': ' + message if message else ''}")
        else:
            if f1 != f2:
                warn(f"Values do not match: {f1} != {f2}{': ' + message if message else ''}")

    def _check_data_equal(self,
                            data1,
                            data2,
                            ignore_hdmf_attrs=False,
                            ignore_string_to_byte=False,
                            message=None):
        """
        Internal helper function used to compare two :py:class:`~hdmf.container.Data` objects

        :param data1: The first :py:class:`~hdmf.container.Data` object
        :type data1: :py:class:`hdmf.container.Data`
        :param data1: The second :py:class:`~hdmf.container.Data` object
        :type data1: :py:class:`hdmf.container.Data
        :param ignore_hdmf_attrs: whether to ignore testing equality of HDMF container attributes, such as
                                    container_source and object_id
        :param ignore_string_to_byte: ignore conversion of str to bytes and compare as unicode instead
        :param message: custom additional message to show when assertions as part of this assert are failing
        """
        if not isinstance(data1, Data):
            warn(f"data1 must be a Data object{': ' + message if message else ''}")
        if not isinstance(data2, Data):
            warn(f"data2 must be a Data object{': ' + message if message else ''}")
        if len(data1) != len(data2):
            warn(f"Data lengths do not match: {len(data1)} != {len(data2)}{': ' + message if message else ''}")
        self._check_array_equal(data1.data, data2.data,
                                    ignore_hdmf_attrs=ignore_hdmf_attrs,
                                    ignore_string_to_byte=ignore_string_to_byte,
                                    message=message)
        self.check_container_equal(container1=data1,
                                    container2=data2,
                                    ignore_hdmf_attrs=ignore_hdmf_attrs,
                                    message=message)

    def _check_array_equal(self,
                            arr1,
                            arr2,
                            ignore_hdmf_attrs=False,
                            ignore_string_to_byte=False,
                            message=None):
        """
        Internal helper function used to check whether two arrays are equal

        :param arr1: The first array
        :param arr2: The second array
        :param ignore_hdmf_attrs: whether to ignore testing equality of HDMF container attributes, such as
                                    container_source and object_id
        :param ignore_string_to_byte: ignore conversion of str to bytes and compare as unicode instead
        :param message: custom additional message to show when assertions as part of this assert are failing
        """
        array_data_types = tuple([i for i in get_docval_macro('array_data')
                                    if (i is not list and i is not tuple and i is not AbstractDataChunkIterator)])
        # We construct array_data_types this way to avoid explicit dependency on h5py, Zarr and other
        # I/O backends. Only list and tuple do not support [()] slicing, and AbstractDataChunkIterator
        # should never occur here. The effective value of array_data_types is then:
        # array_data_types = (np.ndarray, h5py.Dataset, zarr.core.Array, hdmf.query.HDMFDataset)
        if isinstance(arr1, array_data_types):
            arr1 = arr1[()]
        if isinstance(arr2, array_data_types):
            arr2 = arr2[()]
        if not isinstance(arr1, (tuple, list, np.ndarray)) and not isinstance(arr2, (tuple, list, np.ndarray)):
            if isinstance(arr1, (float, np.floating)):
                if not np.allclose(arr1, arr2, equal_nan=True):
                    warn(f"Float values not close: {arr1} != {arr2}{': ' + message if message else ''}")
            else:
                if ignore_string_to_byte:
                    if isinstance(arr1, bytes):
                        arr1 = arr1.decode('utf-8')
                    if isinstance(arr2, bytes):
                        arr2 = arr2.decode('utf-8')
                if arr1 != arr2:
                    warn(f"Scalar values do not match: {arr1} != {arr2}{': ' + message if message else ''}")
        else:
            if len(arr1) != len(arr2):
                warn(f"Array lengths do not match: {len(arr1)} != {len(arr2)}{': ' + message if message else ''}")
            if isinstance(arr1, np.ndarray) and len(arr1.dtype) > 1:  # compound type
                arr1 = arr1.tolist()
            if isinstance(arr2, np.ndarray) and len(arr2.dtype) > 1:  # compound type
                arr2 = arr2.tolist()
            if isinstance(arr1, np.ndarray) and isinstance(arr2, np.ndarray):
                if np.issubdtype(arr1.dtype, np.number):
                    if not np.allclose(arr1, arr2, equal_nan=True):
                        warn(f"Numeric arrays not close{': ' + message if message else ''}")
                else:
                    if not np.array_equal(arr1, arr2):
                        warn(f"Arrays not equal{': ' + message if message else ''}")
            else:
                for sub1, sub2 in zip(arr1, arr2):
                    if isinstance(sub1, Container):
                        self.check_container_equal(sub1, sub2,
                                                    ignore_hdmf_attrs=ignore_hdmf_attrs,
                                                    ignore_string_to_byte=ignore_string_to_byte,
                                                    message=message)
                    elif isinstance(sub1, Data):
                        self._check_data_equal(sub1, sub2,
                                                ignore_hdmf_attrs=ignore_hdmf_attrs,
                                                ignore_string_to_byte=ignore_string_to_byte,
                                                message=message)
                    else:
                        self._check_array_equal(sub1, sub2,
                                                    ignore_hdmf_attrs=ignore_hdmf_attrs,
                                                    ignore_string_to_byte=ignore_string_to_byte,
                                                    message=message)




with NWBHDF5IO(filename, 'r') as read_io:
    nwbfile_hdf5 = read_io.read()

    # use the same BuildManager so that the generated extension classes are the same
    # TODO: but the written extension is not the same as the original extension
    with NWBZarrIO(zarr_filename, mode='r', manager=read_io.manager) as zarr_io:
        nwbfile_zarr = zarr_io.read()

        # Values do not match: None != unknown
        #   is expected for nwbfile.subject.strain.
        NWBContainerComparator().check_container_equal(nwbfile_hdf5, nwbfile_zarr, ignore_hdmf_attrs=True)



