import h5py
import numpy as np
import tables  # so that BLOSC library is available
from copy_utils import *

"""
Read data from an event-mode ISIS NeXus file and write it to a new file in proposed format for ESS
"""


class DatasetToCopy:
    def __init__(self, name, truncate=None, overwrite_with=None):
        self.name = name
        self.truncate = truncate
        if not isinstance(overwrite_with, np.ndarray):
            self.overwrite_with = np.array(overwrite_with)
        else:
            self.overwrite_with = overwrite_with


class DatasetToCreate:
    def __init__(self, name, data, attributes=None):
        self.name = name
        self.attributes = attributes
        if not isinstance(data, np.ndarray):
            self.data = np.array(data)
        else:
            self.data = data


def create_dataset(out_file, dataset_name, data, attributes=None, compress_type='gzip', compress_opts=1):
    if not isinstance(data, np.ndarray):
        print "data argument specified for " + dataset_name + " must be a numpy array"
        return
    target_data = out_file.create_dataset(dataset_name, data.shape, dtype=data.dtype, data=data,
                                          compression=compress_type, compression_opts=compress_opts)
    if attributes:
        try:
            for name, value in attributes.items():
                target_data.attrs[name] = value
        except AttributeError:
            print('ERROR: failed to write attributes for ' + dataset_name)


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
            # copy_group_with_attributes(target_file, source_file, '/raw_data_1/instrument')
            # target_file.copy(source_file['/raw_data_1/instrument/source'], '/raw_data_1/instrument/source')
            for dataset in datasets:
                if isinstance(dataset, DatasetToCopy):
                    copy_dataset_with_attributes(target_file, source_file, dataset.name, truncate=dataset.truncate,
                                                 overwrite_with=dataset.overwrite_with)
                elif isinstance(dataset, DatasetToCreate):
                    create_dataset(target_file, dataset.name, dataset.data, dataset.attributes)


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


def rewrite_entire_file(source_filename, target_filename):
    clear_file(target_filename)
    with h5py.File(source_filename, 'r') as source_file:
        with h5py.File(target_filename, 'r+') as target_file:
            copy_all(target_file, source_file, '/raw_data_1', compress_type='gzip', compress_opts=1)


input_filename = 'data/SANS_test.nxs'
output_filename = 'data/SANS_test_ESS_format.nxs'
reduced_filename = 'data/SANS_test_reduced.nxs'

datasets = [
    DatasetToCopy('/raw_data_1/detector_1_events/event_id', truncate=7814),
    DatasetToCopy('/raw_data_1/detector_1_events/event_index', truncate=10),
    DatasetToCopy('/raw_data_1/detector_1_events/event_time_offset', truncate=7814),
    DatasetToCopy('/raw_data_1/detector_1_events/event_time_zero', truncate=10),
    DatasetToCopy('/raw_data_1/detector_1_events/total_counts', overwrite_with=7814),
    DatasetToCreate('/raw_data_1/detector_1_events/cue_timestamp_zero', [0.05, 0.12, 0.33],
                    {'units': 'second', 'offset': '2016-04-12T02:58:52'}),
    DatasetToCreate('/raw_data_1/detector_1_events/cue_index', [349, 872, 1624])
]

clear_file(output_filename)
# Rewrite file with GZip compression (level 1)
rewrite_to_ess_format(input_filename, output_filename)
# Or, rewrite file with BLOSC compression
# rewrite_to_ess_format(input_filename, output_filename, 32001, None)
create_reduced_file_for_comparison(input_filename, reduced_filename, datasets)

# This is necessary to actually remove truncated data and reduce file size
rewrite_entire_file(reduced_filename, 'data/SANS_test_reduced_small.nxs')
