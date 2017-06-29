import threading
import queue
import time
import calendar
import copy
import numpy as np

configuration = {
    'kalman_url': 'http://www.panga.org/realtime/data/api/',  # Not used, maybe future?
    'kalman_url_en': '?q=5min&l=',  # Not used, maybe future?
    'sites_file': './sta_offset.d',
    'faults_file': './subfaults.d',
    'rabbit_mq': {'exchange_name': 'fastlane-nev-cov',
                  'host': 'pc96445.d.cwu.edu',
                  'userid': 'panga_ro',
                  'password': 'ro',
                  'virtual_host': '/CWU-ppp'
                  },
    'kalman_stale': 30,  # (seconds) Time before kalman states are wiped
    'group_timespan': 1,  # (seconds) Group batches of data in timespan
    'delay_timespan': 15,  # (seconds) Time to wait for laggard data
    'idle_sleep_time': 0.1,  # (seconds) Time to sleep to avoid busy wait loops

    'max_validator_threads': 4,
    'validator_queue_threshold': 1,
    'inverter_queue_threshold': 1,
    'kalman_queue_threshold': 1,
    'max_kalman_threads': 30,
    'max_inverter_threads': 10,

    'kalman_default_lat': -120,
    'kalman_default_lon': 48,

    # specified in config file:
    'minimum_offset': 0.001,  #inverter config/validator/readonceconfig
    'convergence': 320.,    # read once config
    'eq_pause': 10.,
    'eq_threshold': 0.01,
    'strike_slip': False,
    'mes_wait': 2,
    'max_offset': 4000,
    'offset': False,
    'min_r': 0.0001
}

inverter_configuration = {
    'sites': None,
    'faults': None,
    'strike_slip': configuration['strike_slip'],
    'short_smoothing': True,
    'smoothing': True,
    'corner_fix': False,
    'float_equality': 1e-9,
    'offsets_per_site': 3,
    'subfault_len': 60.,
    'subfault_wid': 30.
}

kalman_filter_configuration = {

}

def get_empty_kalman_state(sites, faults):
    delta_t = 1
    kalman_state = {
        'site': None,
        'prev_time': None,
        'delta_t': delta_t,
        'h': np.matrix([[1., 0., 0.], [0., 1., 0.], [0., 0., 1.]]),
        'phi': np.matrix([[1., 0., 0.], [0., 1., 0.], [0., 0., 1.]]),
        'state': np.matrix([[0.], [0.], [0.]]),
        'state_2': np.matrix([[0.], [0.], [0.]]),
        'max_offset': configuration['max_offset'],
        'iden': np.matrix([[1., 0., 0.], [0., 1., 0.], [0., 0., 1.]]),
        'k': np.matrix([[0., 0., 0.], [0., 0., 0.], [0., 0., 0.]]),
        'm': np.matrix([[0., 0., 0.], [0., 0., 0.], [0., 0., 0.]]),
        'p': np.matrix([[1000., 0., 0.], [0., 1000., 0.], [0., 0., 1000.]]),
        'last_calculation': None,
        'measurement_queue': queue.PriorityQueue(),
        'lock': threading.Lock(),
        'sites': sites,
        'faults': faults,
        'measurement_matrix': np.matrix([[0.], [0.], [0.]]),
        'r': np.matrix([[0., 0., 0.], [0., 0., 0.], [0., 0., 0.]]),
        'def_r': configuration['min_r'],
        'offset': configuration['offset'],
        'synth_n': 0.,
        'synth_e': 0.,
        'synth_v': 0.,
        'i_state': np.matrix([[0.], [0.], [0.]]),
        'i_state_2': np.matrix([[0.], [0.], [0.]]),
        'q': np.matrix([[delta_t, 0., 0.], [0., delta_t, 0.], [0., 0., delta_t]]),
        'res': np.matrix([[0.], [0.], [0.]]),
        'override_flag': False,
        'sm_count': 0,
        'smoothing': configuration['eq_pause'],
        'start_up': True,
        'eq_count': np.matrix([[0], [0], [0]]),
        'eq_flag': np.matrix([[False], [False], [False]]),
        'eq_threshold': configuration['eq_threshold'],
        'tag': False,
        's_measure': [],
        'init_p': 0,
        'reset_p': np.matrix([[1000., 0., 0.], [0., 1000., 0.], [0., 0., 1000.]]),
        'p_count': 0,
        'time': 0,
        'write': '',
        'data_set': [],
        'wait': configuration['mes_wait']
    }
    return kalman_state


def get_empty_inverter_state(sites, faults):
    inverter_state = {
        'sub_inputs': None,
        'alpha': 1.0,
        'cutoff': 0.,
        'noise': 0.,
        'convergence': -1.,
        'min_offset': -1.,
        'num_faults': -1.,
        'range_threshold': -1.,
        'subfault_width': 30.,
        'subfault_length': 60.,
        'label': "",
        'tag': ""
    }


station = {
    'name': '',
    'lat': '',
    'lon': '',
    'height': '',
    'index': 0
}

faults = {
    'length': '',
    'width': '',
    'subfault_list': []
}

''' this is a sample data set from a measurement for reference:
measurement_definition = {
    'cnv': -0.933522187282139,
    'e': -0.145276922321984,
    'cn': 0.80534707627878,
    'site': 'TGMX',
    'ce': 0.816841136571193,
    'n': -0.085254873056399,
    'cev': -1.184385979619853,
    'cne': 0.109459815502798,
    't': 1492637775,
    'v': 0.002735296210888,
    'cv': 6.907785271563459
}'''

gps_measurement_message_definition = {
    'gps_data': None,
    'gps_data_timestamp': None,
    'gps_data_sequence_number': None,
    'timestamps': {
        'data_received': None,
        'kalman_verified': None,
        'kalman_begin': None,
    },
    'data_events': [],
    'configuration': None
}

def get_gps_measurement_queue_message(prev_message=None, kalman_verify_step=False, kalman_filter_step=False,
                               gps_data=None, gps_data_sequence_number=None, kalman_data=None):
    if prev_message is None:
        prev_message = copy.deepcopy(gps_measurement_message_definition)
    queue_message = {
        'gps_data': prev_message['gps_data'] if gps_data is None else gps_data,
        'gps_data_timestamp': prev_message['gps_data_timestamp'] if gps_data is None else gps_data['t'],
        'gps_data_sequence_number': prev_message['gps_data_sequence_number'] if gps_data is None
        else gps_data_sequence_number,
        'timestamps': {
            'data_received': calendar.timegm(time.gmtime()) if prev_message is not None
            else prev_message['queue_message']['data_received'],
            'kalman_verified': calendar.timegm(time.gmtime()) if kalman_verify_step is True
            else prev_message['timestamps']['kalman_verified'],
            'kalman_begin': calendar.timegm(time.gmtime()) if kalman_filter_step is True
            else prev_message['timestamps']['kalman_begin']
        },
        'configuration': configuration
    }
    return queue_message

kalman_output_definition = {
    'site': None,
    'la': None,
    'lo': None,
    'mn': None,
    'me': None,
    'mv': None,
    'kn': None,
    'ke': None,
    'kv': None,
    'cn': None,
    'ce': None,
    'cv': None,
    'he': None,
    'ta': None,
    'st': None,
    'time': None,
}

grouped_inversion_queue_message_definition = {
    'time_group': None,
    'sequence_number': None,
    'kalman_output_data': {},   # key: site name, value kalman_output_definition
    'gps_measurement_data': None,
    'inverter_output_data': None,
    'slip_output_data': None,
    'timestamps': {
        'data_received': None,
        'kalman_verified': None,
        'kalman_begin': None,
        'kalman_output_sent': None,
        'invert_timegroup_queued': None,
        'invert_begin': None,
        'invert_end': None,
        'output_sent': None
    },
    'data_events': [],
    'configuration': None
}

def get_grouped_inversion_queue_message(prev_message=None,
                                        gps_measurement_data=None,
                                        kalman_output_data=None,
                                        inverter_output_data=None,
                                        slip_output_data=None,
                                        inverter_begin=False,
                                        inverter_wait_queue_dequeue=False,
                                        configuration=None):
    if prev_message is None:
        prev_message = copy.deepcopy(grouped_inversion_queue_message_definition)
    if gps_measurement_data is None:
        time_group = prev_message['time_group']
        sequence_number = prev_message['sequence_number']
        data_received = prev_message['timestamps']['data_received']
        kalman_begin = prev_message['timestamps']['kalman_begin']
        kalman_verified = prev_message['timestamps']['kalman_verified']
        gps_measurement_data = prev_message['gps_measurement_data']
    else:
        measurement = list(gps_measurement_data.keys())[0]
        time_group = gps_measurement_data[measurement][0]['gps_data_timestamp']
        sequence_number = gps_measurement_data[measurement][0]['gps_data_sequence_number']
        data_received = gps_measurement_data[measurement][0]['timestamps']['data_received']
        kalman_verified = gps_measurement_data[measurement][0]['timestamps']['kalman_verified']
        kalman_begin = gps_measurement_data[measurement][0]['timestamps']['kalman_begin']

    queue_message = {
        'gps_measurement_data': gps_measurement_data,
        'time_group': time_group,
        'sequence_number': sequence_number,
        'kalman_output_data': prev_message['kalman_output_data'] if kalman_output_data is None else
                              kalman_output_data,
        'inverter_output_data': prev_message['inverter_output_data'] if inverter_output_data is None else
                                inverter_output_data,
        'slip_output_data': prev_message['slip_output_data'] if slip_output_data is None else slip_output_data,
        'timestamps': {
            'data_received': data_received,
            'kalman_verified': kalman_verified,
            'kalman_begin': kalman_begin,
            'kalman_output_sent': calendar.timegm(time.gmtime()) if kalman_output_data is not None
            else prev_message['timestamps']['kalman_output_sent'],
            'invert_timegroup_queued': calendar.timegm(time.gmtime()) if inverter_wait_queue_dequeue is True
            else prev_message['timestamps']['invert_timegroup_queued'],
            'invert_begin': calendar.timegm(time.gmtime()) if inverter_begin is True
            else prev_message['timestamps']['invert_begin'],
            'invert_end': calendar.timegm(time.gmtime()) if inverter_output_data is not None
            else prev_message['timestamps']['invert_end'],
            'output_sent': calendar.timegm(time.gmtime()) if slip_output_data is not None
            else prev_message['timestamps']['output_sent']
        },
        'configuration': configuration
    }
    return queue_message

