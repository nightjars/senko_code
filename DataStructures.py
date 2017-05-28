import threading
import queue
import time
import calendar
import copy
import numpy as np

configuration = {
    'kalman_url': 'http://www.panga.org/realtime/data/api/',  # Not used, maybe future?
    'kalman_url_en': '?q=5min&l=',  # Not used, maybe future?
    'sites_file': './sta_offset2.d',
    'faults_file': './subfaults.d',
    'rabbit_mq': {'exchange_name': 'fastlane-nev-cov',
                  'host': 'pc96445.d.cwu.edu',
                  'userid': 'panga_ro',
                  'password': 'ro',
                  'virtual_host': '/CWU-ppp'
                  },
    'kalman_stale': 300,  # (seconds) Time before kelman states are wiped
    'group_timespan': 1,  # (seconds) Group batches of data in timespan
    'delay_timespan': 15,  # (seconds) Time to wait for laggard data
    'idle_sleep_time': 0.1,  # (seconds) Time to sleep to avoid busy wait loops

    'minimum_offset': -1.,  # for validator

    'max_validator_threads': 4,
    'validator_queue_threshold': 1,
    'inverter_queue_threshold': 1,
    'kalman_queue_threshold': 1,
    'max_kalman_threads': 30,
    'max_inverter_threads': 10,

    'kalman_default_lat': -120,
    'kalman_default_lon': 48,
    'offsets_per_site': 3,
    'subfault_len': 60.,
    'subfault_wid': 30.,
    'smoothing': True,
    'corner_fix': True,
    'short_smoothing': True,
    'strike_slip': False,
    'float_equality': 1e-9
}


def get_empty_kalman_state(sites, faults):
    delta_t = 1
    kalman_state = {
        'site': None,
        'delta_t': delta_t,
        'h': np.matrix([[1., 0., 0.], [0., 1., 0.], [0., 0., 1.]]),
        'phi': np.matrix([[1., 0., 0.], [0., 1., 0.], [0., 0., 1.]]),
        'state': np.matrix([[0.], [0.], [0.]]),
        'state_2': np.matrix([[0.], [0.], [0.]]),
        'max_offset': 25.0,
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
        'def_r': 0.0001,
        'offset': False,
        'synth_n': 0.,
        'synth_e': 0.,
        'synth_v': 0.,
        'i_state': np.matrix([[0.], [0.], [0.]]),
        'i_state_2': np.matrix([[0.], [0.], [0.]]),
        'q': np.matrix([[delta_t, 0., 0.], [0., delta_t, 0.], [0., 0., delta_t]]),
        'res': np.matrix([[0.], [0.], [0.]]),
        'override_flag': False,
        'sm_count': 0,
        'smoothing': 60,
        'start_up': True,
        'eq_count': np.matrix([[0], [0], [0]]),
        'eq_flag': np.matrix([[False], [False], [False]]),
        'eq_threshold': 0.001,
        'tag': False,
        's_measure': [],
        'init_p': 0,
        'reset_p': np.matrix([[1000., 0., 0.], [0., 1000., 0.], [0., 0., 1000.]]),
        'p_count': 0,
        'time': 0,
        'write': '',
        'data_set': [],
        'wait': 2
    }
    return kalman_state


def get_empty_inverter_state(sites, faults):
    inverter_state = {
        'sites': sites,
        'faults': faults,
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
    'minimum_offset': '',
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

gps_data_queue_message_definition = {
    'gps_data': None,
    'gps_data_timestamp': None,
    'gps_data_sequence_number': None,
    'timestamps': {
        'data_received': None,
        'kalman_verified': None,
        'kalman_begin': None,
    },
    'data_events': []
}

post_kalman_queue_message_definition = {
    'time_group': None,
    'sequence_number': None,
    'pre_kalman': None,
    'kalman_data': {
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
    },
    'timestamps': {
        'kalman_sent': None,
    },
    'data_events': []
}

grouped_inversion_queue_message_definition = {
    'time_group': None,
    'kalman_data': None,
    'inverter_data': None,
    'output_data': None,
    'timestamps': {
        'invert_timegroup_queued': None,
        'invert_begin': None,
        'invert_end': None,
        'output_sent': None
    },
    'data_events': []
}


def get_gps_data_queue_message(prev_message=None, kalman_verify_step=False, kalman_filter_step=False,
                               gps_data=None, gps_data_sequence_number=None, kalman_data=None):
    if prev_message is None:
        prev_message = copy.deepcopy(gps_data_queue_message_definition)
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
        }
    }
    return queue_message


def get_post_kalman_queue_message(prev_message=None, pre_kalman=None, kalman_data=None):
    if prev_message is None:
        prev_message = copy.deepcopy(post_kalman_queue_message_definition)
    if pre_kalman is not None:
        time_group = 0
        sequence_number = 0
        for calculation in pre_kalman:
            time_group = max(time_group, calculation['gps_data_timestamp'])
        # No need for the newest number for this, it is used just as a tie-breaker for priority queue
        sequence_number = pre_kalman[-1]['gps_data_sequence_number']
    else:
        pre_kalman = prev_message['pre_kalman']
        time_group = prev_message['time_group']
        sequence_number = prev_message['sequence_number']
    queue_message = {
        'pre_kalman': pre_kalman,
        'time_group': time_group,
        'sequence_number': sequence_number,
        'kalman_data': prev_message['kalman_data'] if kalman_data is None else kalman_data,
        'timestamps': {
            'kalman_sent': calendar.timegm(time.gmtime()) if kalman_data is not None
            else prev_message['timestamps']['kalman_sent'],
        }
    }
    return queue_message


def get_grouped_inversion_queue_message(prev_message=None, kalman_data=None, inverter_data=None,
                                        output_data=None, inverter_begin=False, inverter_wait_queue_dequeue=False,
                                        inverter_wait_queue_enqeue=False):
    if prev_message is None:
        prev_message = copy.deepcopy(grouped_inversion_queue_message_definition)
    if kalman_data is not None:
        # Using index 0 since all data sets will be same time group (if by 1 - will fix later)
        time_group = kalman_data[0]['time_group']
        kalman_data = kalman_data
    else:
        time_group = prev_message['time_group']
        kalman_data = prev_message['kalman_data']
    queue_message = {
        'time_group': time_group,
        'kalman_data': kalman_data,
        'inverter_data': prev_message['inverter_data'] if inverter_data is None else inverter_data,
        'output_data': prev_message['output_data'] if output_data is None else output_data,
        'timestamps': {
            'invert_timegroup_queued': calendar.timegm(time.gmtime()) if inverter_wait_queue_dequeue is True
            else prev_message['timestamps']['invert_timegroup_queued'],
            'invert_begin': calendar.timegm(time.gmtime()) if inverter_begin is True
            else prev_message['timestamps']['invert_begin'],
            'invert_end': calendar.timegm(time.gmtime()) if inverter_data is not None
            else prev_message['timestamps']['invert_end'],
            'output_sent': calendar.timegm(time.gmtime()) if output_data is not None
            else prev_message['timestamps']['output_sent']
        }
    }
    return queue_message
