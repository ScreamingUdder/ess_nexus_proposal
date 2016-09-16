import h5py
import numpy as np
import tables

"""
Read data from an event-mode ISIS NeXus file and write it to a new file in proposed format for ESS
"""


def clear_file(filename):
    """
    Clear the file or create it if it does not yet exist
    :param filename: name of the NeXus file to clear
    """
    with h5py.File(filename, 'w') as clear_file:
        pass


def copy_group_with_attributes(out_file, in_file, group_name):
    out_file.copy(in_file[group_name], group_name, shallow=True)
    for sub_group in in_file[group_name].keys():
        if sub_group in out_file[group_name]:
            out_file[group_name].__delitem__(sub_group)


def copy_dataset_with_attributes(out_file, in_file, source_dataset, compress_type='gzip',
                                 compress_opts=1, target_dataset=None):
    if not target_dataset:
        target_dataset = source_dataset
    data = in_file.get(source_dataset)
    target_data = out_file.create_dataset(target_dataset, data[...].shape, dtype=data.dtype,
                                          compression=compress_type, compression_opts=compress_opts)
    target_data[...] = data[...]
    target_data.attrs = data.attrs


def copy_object(name):
    pass
    # do copy_group_with_attributes if it is a group
    # do copy_dataset_with_attributes if it is a dataset


def copy_all(out_file, in_file, group_name, compress_type='gzip', compress_opts=1):
    in_group = in_file[group_name]
    group = out_file.create_group(group_name)
    group.attrs = in_file[group_name].attrs
    in_group.visit(copy_object)


def create_reduced_file_for_comparison(source_filename, target_filename, datasets):
    """
    Create a reduced version of the original file to compare in size to the ESS format file
    :param source_filename: name of the original ISIS NeXus file
    :param target_filename: name of the output reduced NeXus file
    :param datasets: list of datasets to copy to output file
    """
    clear_file(target_filename)
    with h5py.File(source_filename, 'r') as source_file:
        with h5py.File(target_filename, 'r+') as target_file:
            copy_group_with_attributes(target_file, source_file, '/raw_data_1')
            copy_group_with_attributes(target_file, source_file, '/raw_data_1/detector_1_events')
            copy_group_with_attributes(target_file, source_file, '/raw_data_1/instrument')
            target_file.copy(source_file['/raw_data_1/instrument/source'], '/raw_data_1/instrument/source')
            for dataset in datasets:
                target_file.copy(source_file[dataset], dataset)


def rewrite_to_ess_format(source_filename, target_filename, compress_type='gzip', compress_opts=1):
    """
    Rewrite source file to another NeXus file with the proposed ESS format
    :param source_filename: name of the original ISIS NeXus file
    :param target_filename: name of the output ESS NeXus file
    :param compress_type: id of HDF5 registered compression filter, defaults to gzip
    :param compress_opts: options to pass to compression filter, defaults to gzip level 1
    """
    with h5py.File(source_filename, 'r') as source_file:
        with h5py.File(target_filename, 'w') as target_file:
            entry_group = target_file.create_group('entry')
            entry_group.attrs['NX_class'] = 'NXentry'

            instr_group = entry_group.create_group('instrument')
            instr_group.attrs['NX_class'] = 'NXinstrument'

            target_file.copy(source_file['/raw_data_1/instrument/source'], '/raw_data_1/instrument/source')

            det_group = instr_group.create_group('detector_1')
            det_group.attrs['NX_class'] = 'NXdetector'

            event_group = det_group.create_group('detector_1_events')
            event_group.attrs['NX_class'] = 'NXlog'

            data_value = source_file.get('/raw_data_1/detector_1_events/event_id')
            event_value = event_group.create_dataset('value', data_value[...].shape, dtype=data_value.dtype,
                                                     compression=compress_type, compression_opts=compress_opts)
            event_value[...] = data_value[...]

            data_time = source_file.get('/raw_data_1/detector_1_events/event_time_offset')
            event_time = event_group.create_dataset('time', data_time[...].shape, dtype=np.dtype('u4'),
                                                    compression=compress_type, compression_opts=compress_opts)
            event_time[...] = data_time[...] * 1000
            event_time.attrs['relative_to'] = 'step'
            event_time.attrs['tick_length'] = 1  # nanoseconds

            pulse_time = source_file.get('/raw_data_1/detector_1_events/event_time_zero')
            step_time = event_group.create_dataset('step_time', pulse_time[...].shape, dtype=np.dtype('u4'),
                                                   compression=compress_type, compression_opts=compress_opts)
            step_time[...] = pulse_time[...] * 100
            step_time.attrs['tick_length'] = 10000000  # nanoseconds (10 milliseconds)

            event_index = source_file.get('/raw_data_1/detector_1_events/event_index')
            step_index = event_group.create_dataset('step_index', event_index[...].shape, dtype=event_index.dtype,
                                                    compression=compress_type, compression_opts=compress_opts)
            step_index[...] = event_index[...]


input_filename = 'data/SANS_test.nxs'
output_filename = 'data/SANS_test_ESS_format.nxs'
reduced_filename = 'data/SANS_test_reduced.nxs'

datasets_transferred = [
    '/raw_data_1/detector_1_events/event_id',
    '/raw_data_1/detector_1_events/event_index',
    '/raw_data_1/detector_1_events/event_time_offset',
    '/raw_data_1/detector_1_events/event_time_zero'
]

clear_file(output_filename)
# Rewrite file with GZip compression (level 1)
rewrite_to_ess_format(input_filename, output_filename)
# Or, rewrite file with BLOSC compression
# rewrite_to_ess_format(input_filename, output_filename, 32001, None)
create_reduced_file_for_comparison(input_filename, reduced_filename, datasets_transferred)
