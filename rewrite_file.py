import h5py

"""
Read data from an event-mode ISIS NeXus file and write it to a new file in proposed format for ESS
"""

source_filename = 'data/SANS_test.nxs'
target_filename = 'data/SANS_test_ESS_format.nxs'

datasets = [
    '/raw_data_1/detector_1_events/event_id',
    '/raw_data_1/detector_1_events/event_index',
    '/raw_data_1/detector_1_events/event_time_offset'
    '/raw_data_1/detector_1_events/event_time_zero'
]

# Clear the output file if it already exists
with h5py.File(target_filename, 'w') as clear_file:
    pass

# Rewrite data using ESS format
with h5py.File(source_filename, 'r') as source_file:
    with h5py.File(target_filename, 'w') as target_file:
        entry_group = target_file.create_group('entry')
        entry_group.attrs['NX_class'] = 'NXentry'

        instr_group = entry_group.create_group('instrument')
        instr_group.attrs['NX_class'] = 'NXinstrument'

        det_group = instr_group.create_group('detector_1')
        det_group.attrs['NX_class'] = 'NXdetector'

        event_group = det_group.create_group('detector_1_events')
        event_group.attrs['NX_class'] = 'NXlog'

        data_value = source_file.get('/raw_data_1/detector_1_events/event_id')
        event_value = event_group.create_dataset('value', data_value[...].shape, dtype=data_value.dtype)
        event_value[...] = data_value[...]

        data_time = source_file.get('/raw_data_1/detector_1_events/event_time_offset')
        event_time = event_group.create_dataset('time', data_time[...].shape, dtype=data_time.dtype)
        event_time[...] = data_time[...]

        pulse_time = source_file.get('/raw_data_1/detector_1_events/event_time_zero')
        step_time = event_group.create_dataset('step_time', pulse_time[...].shape, dtype=pulse_time.dtype)
        step_time[...] = pulse_time[...]

        event_index = source_file.get('/raw_data_1/detector_1_events/event_index')
        step_index = event_group.create_dataset('step_index', event_index[...].shape, dtype=event_index.dtype)
        step_index[...] = event_index[...]

# Create a reduced version of the original file to compare in size to the ESS format file
    # for dataset in datasets:
    #     data_1 = source_file.get(dataset)
    #
    #     with h5py.File(target_filename, 'r+') as target_file:
    #         print(data_1[...])
    #         target_file[dataset] = data_1[...]
