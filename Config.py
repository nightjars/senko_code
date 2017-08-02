import threading
import queue
import time
import calendar
import copy
import numpy as np
import Inverter

configuration = {
    'kalman_url': 'http://www.panga.org/realtime/data/api/',  # Not used, maybe future?
    'kalman_url_en': '?q=5min&l=',  # Not used, maybe future?
    'rabbit_mq_input': {'exchange_name': 'fastlane-nev-cov',
                  'host': 'pc96445.d.cwu.edu',
                  'userid': 'panga_ro',
                  'password': 'ro',
                  'virtual_host': '/CWU-ppp'},
    'rabbit_mq_output': {'exchange_name': 'slip-inversion2',
                         'host': 'pc96225.d.cwu.edu',
                         'port': 5672,
                         'userid': 'nif',
                         'virtual_host': '/rtgps-products',
                         'password': 'nars0add',
                         'model': 'Test'},
    'mongo_db_output': {'host': 'pc96225.d.cwu.edu',
                        'port': 27018,
                        'userid': 'nif',
                        'password': 'nars0add'},
    'delay_timespan': 15,  # (seconds) Time to wait for laggard data

    'max_kalman_threads': 30,
    'max_inverter_threads': 1,

    'kalman_default_lat': -120,
    'kalman_default_lon': 48,
    'output_enabled': False
}

inversion_runs = []
queue_manager = None

def add_inversion_run(run):
    Inverter.config_generator(run)
    run['filters'] = {}
    inversion_runs.append(run)

def remove_inversion_run(run):
    inversion_runs.remove(run)

def get_empty_kalman_state(run):
    delta_t = 1
    kalman_state = {
        'site': None,
        'prev_time': None,
        'delta_t': delta_t,
        'h': np.matrix([[1., 0., 0.], [0., 1., 0.], [0., 0., 1.]]),
        'phi': np.matrix([[1., 0., 0.], [0., 1., 0.], [0., 0., 1.]]),
        'state': np.matrix([[0.], [0.], [0.]]),
        'state_2': np.matrix([[0.], [0.], [0.]]),
        'max_offset': run['max_offset'],
        'iden': np.matrix([[1., 0., 0.], [0., 1., 0.], [0., 0., 1.]]),
        'k': np.matrix([[0., 0., 0.], [0., 0., 0.], [0., 0., 0.]]),
        'm': np.matrix([[0., 0., 0.], [0., 0., 0.], [0., 0., 0.]]),
        'p': np.matrix([[1000., 0., 0.], [0., 1000., 0.], [0., 0., 1000.]]),
        'last_calculation': None,
        'measurement_queue': queue.PriorityQueue(),
        'lock': threading.Lock(),
        'sites': run['sites'],
        'faults': faults,
        'measurement_matrix': np.matrix([[0.], [0.], [0.]]),
        'r': np.matrix([[0., 0., 0.], [0., 0., 0.], [0., 0., 0.]]),
        'def_r': run['min_r'],
        'offset': run['offset'],
        'synth_n': 0.,
        'synth_e': 0.,
        'synth_v': 0.,
        'i_state': np.matrix([[0.], [0.], [0.]]),
        'i_state_2': np.matrix([[0.], [0.], [0.]]),
        'q': np.matrix([[delta_t, 0., 0.], [0., delta_t, 0.], [0., 0., delta_t]]),
        'res': np.matrix([[0.], [0.], [0.]]),
        'override_flag': False,
        'sm_count': 0,
        'smoothing': run['eq_pause'],
        'start_up': True,
        'eq_count': np.matrix([[0], [0], [0]]),
        'eq_flag': np.matrix([[False], [False], [False]]),
        'eq_threshold': run['eq_threshold'],
        'tag': False,
        's_measure': [],
        'init_p': 0,
        'reset_p': np.matrix([[1000., 0., 0.], [0., 1000., 0.], [0., 0., 1000.]]),
        'p_count': 0,
        'time': 0,
        'write': '',
        'data_set': [],
        'wait': run['mes_wait'],
        'run': run,
        'temp_kill_limit': 100,
        'temp_kill': 0          # temporary solution for problem with kalman filter state/state2 causing measurements to be ignored                                
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

