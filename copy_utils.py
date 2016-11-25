import h5py
import numpy as np
import tables  # so that BLOSC library is available

"""
Utilities for copying data from one HDF5 file to another
"""


def copy_group_with_attributes(out_file, in_file, group_name):
    out_file.copy(in_file[group_name], group_name, shallow=True)
    for sub_group in in_file[group_name].keys():
        if sub_group in out_file[group_name]:
            out_file[group_name].__delitem__(sub_group)


def copy_dataset_with_attributes(out_file, in_file, source_dataset, compress_type='gzip',
                                 compress_opts=1, target_dataset=None, truncate=None):
    if not target_dataset:
        target_dataset = source_dataset
    data = in_file.get(source_dataset)
    if not truncate:
        target_data = out_file.create_dataset(target_dataset, data[...].shape, dtype=data.dtype,
                                              compression=compress_type, compression_opts=compress_opts)
        target_data[...] = data[...]
    else:
        target_data = out_file.create_dataset(target_dataset, data[:truncate].shape, dtype=data.dtype,
                                              compression=compress_type, compression_opts=compress_opts)
        target_data[...] = data[:truncate]
    try:
        for name, value in data.attrs.items():
            target_data.attrs[name] = value
    except AttributeError:
        print('ERROR: cannot copy attributes from ' + source_dataset)


def is_dataset(item):
    """
    Return true if item is an h5py.Dataset
    """
    return isinstance(item, h5py.Dataset)


def is_group(item):
    """
    Return true if item is an h5py.Group
    """
    return isinstance(item, h5py.Group)


def clear_file(filename):
    """
    Clear the file or create it if it does not yet exist
    :param filename: name of the NeXus file to clear
    """
    with h5py.File(filename, 'w') as clear_file:
        pass


def copy_all(out_file, in_file, group_name, compress_type='gzip', compress_opts=1):
    """
    Copy all groups and datasets in group_name, with their attributes,
    from in_file to out_file with the given compression options.
    See rewrite_file.py for example of how to use.

    :param out_file: output file object
    :param in_file: input file object
    :param group_name: name of the group to completely copy
    :param compress_type: compression type, for example 'gzip', 'None', '32001'
    :param compress_opts: compression options for example compression level for gzip
    """

    def copy_object(name):
        full_name = group_name + '/' + name
        if is_group(in_file[full_name]):
            copy_group_with_attributes(out_file, in_file, full_name)
        elif is_dataset(in_file[full_name]):
            copy_dataset_with_attributes(out_file, in_file, full_name, compress_type, compress_opts)
        else:
            print('ERROR: ' + full_name + ' is apparently not a group or dataset... is it a link?')

    copy_group_with_attributes(out_file, in_file, group_name)
    in_group = in_file[group_name]
    in_group.visit(copy_object)
