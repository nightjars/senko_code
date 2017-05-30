import threading
import DataStructures
import time
import random
import calendar
import numpy as np
import logging
import multiprocessing
import queue


class KalmanThread(threading.Thread):
    def __init__(self, input_queue, output_queue):
        self.logger = logging.getLogger(__name__)
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.terminated = False
        threading.Thread.__init__(self)

    def run(self):
        while not self.terminated:
            try:
                kalman = self.input_queue.get(timeout=1)
                if kalman['lock'].acquire(False):
                    self.logger.debug("{} beginning processing {}".format(self, kalman['site']))
                    while not self.terminated and not kalman['measurement_queue'].empty():
                        try:
                            (_, _, measurement) = kalman['measurement_queue'].get(timeout=1)
                            kalman['data_set'].append(DataStructures.get_gps_data_queue_message(measurement,
                                                        kalman_filter_step=True))
                            self.process_measurement(kalman)
                        except queue.Empty:
                            pass
                    kalman['lock'].release()
                    self.logger.debug("{} finished processing {}".format(self, kalman['site']))

            except queue.Empty:
                pass

    def process_measurement(self, kalman):
        if kalman['last_calculation'] is None or \
                        kalman['data_set'][-1]['gps_data']['t'] > kalman['last_calculation']:
            gps_data = kalman['data_set'][-1]['gps_data']
            kalman['time'] = gps_data['t']
            if gps_data['n'] != 0. or gps_data['e'] != 0. or gps_data['v'] != 0.:
                cn = gps_data['cn']
                cv = gps_data['cv']
                ce = gps_data['ce']
                n = gps_data['n'] + kalman['synth_n']
                e = gps_data['e'] + kalman['synth_e']
                v = gps_data['v'] + kalman['synth_v']
                r = np.matrix([[cn, 0., 0.], [0., ce, 0.], [0., 0., cv]])
                measure_matrix = np.matrix([[n], [e], [v]])
                res = measure_matrix - kalman['h'] * kalman['phi'] * kalman['state'] -      \
                                       kalman['h'] * kalman['phi'] * kalman['state_2']
                if np.abs(res[0, 0]) < kalman['max_offset'] and np.abs(res[1, 0]) < kalman['max_offset'] and   \
                            np.abs(res[2, 0]) < kalman['max_offset']:
                    if kalman['last_calculation'] is None:
                        kalman['site'] = kalman['sites'][kalman['data_set'][-1]['gps_data']['site']]
                        kalman['state_2'] = measure_matrix * 1.0   # what's with the * 1.0?
                    else:
                        kalman['measurement_matrix'] = measure_matrix
                        r[0, 0] = max(r[0, 0], kalman['def_r'])
                        r[1, 1] = max(r[1, 1], kalman['def_r'])
                        r[2, 2] = max(r[2, 2], kalman['def_r'])
                        kalman['r'] = r
                        if kalman['offset']:
                            self.pass_update_state(kalman)
                        else:
                            self.update_matrix(kalman)

                kalman['last_calculation'] = kalman['data_set'][-1]['gps_data']['t']

    def update_matrix(self, kalman):
        if kalman['last_calculation'] is not None:
            kalman['delta_t'] = kalman['data_set'][-1]['gps_data_timestamp'] - kalman['last_calculation']
            kalman['last_calculation'] = kalman['time']
            kalman['q'] = np.matrix([[kalman['delta_t'], 0., 0.],
                                     [0., kalman['delta_t'], 0.],
                                     [0., 0., kalman['delta_t']]])
        kalman['m'] = kalman['phi'] * kalman['p'] * kalman['phi'].T + kalman['q']
        kalman['k'] = kalman['m'] * kalman['h'].T *                                     \
                      (kalman['h'] * kalman['m'] * kalman['h'].T + kalman['r']).I
        kalman['p'] = (kalman['iden'] - kalman['k'] * kalman['h']) * kalman['m']
        self.calc_res(kalman)

    def calc_res(self, kalman):
        kalman['res'] = kalman['measurement_matrix'] - kalman['h'] * kalman['phi'] * kalman['state'] -    \
            kalman['h'] * kalman['phi'] * kalman['state_2']
        if not kalman['override_flag']:
            self.determine_state(kalman)

    def determine_state(self, kalman):
        if kalman['sm_count'] >= kalman['smoothing'] and kalman['start_up']:
            kalman['start_up'] = False
        if kalman['sm_count'] < kalman['smoothing']:
            kalman['eq_count'] = np.matrix([ [0], [0], [0] ])
            self.normal_mode(kalman)
            self.end_proc(kalman)
        else:
            eq_flag = kalman['eq_flag']
            eq_count = kalman['eq_count']
            res = kalman['res']
            r = kalman['r']
            if np.abs(kalman['res'][0,0]) < np.sqrt(kalman['r'][0,0]) * kalman['eq_threshold']:
                kalman['eq_flag'][0,0] = False
                kalman['eq_count'][0,0] = 0
            else:
                kalman['eq_flag'][0,0] = True
                kalman['eq_count'][0,0] += 1
            if np.abs(kalman['res'][1,0]) < np.sqrt(kalman['r'][1,1]) * kalman['eq_threshold']:
                kalman['eq_flag'][1,0] = False
                kalman['eq_count'][1,0] = 0
            else:
                kalman['eq_flag'][1,0] = True
                kalman['eq_count'][1,0] += 1
            if np.abs(kalman['res'][2,0]) < np.sqrt(kalman['r'][2,2]) * kalman['eq_threshold']:
                kalman['eq_flag'][2,0] = False
                kalman['eq_count'][2,0] = 0
            else:
                kalman['eq_flag'][2,0] = True
                kalman['eq_count'][2,0] += 1
            if self.eq_flag_test(kalman) and self.eq_num_test(kalman) > kalman['wait'] and kalman['offset']:
                self.eq_state(kalman)
            elif kalman['offset'] and not self.eq_flag_test(kalman):
                self.false_eq_state(kalman)
            elif self.eq_flag_test(kalman) and not kalman['offset']:
                self.begin_eq_test_state(kalman)
            else:
                self.normal_mode(kalman)
                self.end_proc(kalman)

    def normal_mode(self, kalman):
        kalman['state'] = kalman['phi'] * kalman['state'] + kalman['k'] * kalman['res']
        kalman['state_2'] = kalman['phi'] * kalman['state_2']
        kalman['tag'] = True if kalman['sm_count'] < kalman['smoothing'] and not kalman['start_up']   \
                        else False
        #self.send stuff in here, not sure if needed

    def eq_state(self, kalman):
        self.offset_reset(kalman)
        kalman['sm_count'] = 0
        kalman['s_measure'].append((kalman['data_set'][-1]['gps_data_timestamp'],
                                    kalman['measurement_matrix'], kalman['r']))
        kalman['init_p'] = kalman['p'][0, 0]
        kalman['p'] = kalman['reset_p'] * 1.0
        kalman['p_count'] = 0.
        kalman['offset'] = False
        kalman['override_flag'] = True
        for x in kalman['s_measure'][:-1]:
            kalman['time'], kalman['measure_matrix'], kalman['r'] = x
            self.update_matrix(kalman)
            self.normal_mode(kalman)
        kalman['time'], kalman['measure_matrix'], kalman['r'] = kalman['s_measure'][-1]
        kalman['s_measure'] = []
        kalman['write'] = True
        kalman['override_flat'] = False

    def false_eq_state(self, kalman):
        kalman['write'] = True
        self.end_pass_state(kalman)
        kalman['override_flag'] = True
        kalman['s_measure'].append((kalman['data_set'][-1]['gps_data_timestamp'],
                                    kalman['measurement_matrix'], kalman['r']))
        for x in kalman['s_measure'][:-1]:
            kalman['time'], kalman['measure_matrix'], kalman['r'] = x
            self.calc_res(kalman)
            self.normal_mode(kalman)
        kalman['time'], kalman['measure_matrix'], kalman['r'] = kalman['s_measure'][-1]
        kalman['s_measure'] = []
        kalman['offset'] = False
        kalman['override_flag'] = False

    def begin_eq_test_state(self, kalman):
        kalman['offset'] = True
        self.pass_state_start(kalman)
        kalman['write'] = False

    def end_proc(self, kalman):
        if not kalman['offset']:
            kalman['sm_count'] += 1
            self.normal_mode(kalman)
        else:
            kalman['s_measure'].append((kalman['data_set'][-1]['gps_data_timestamp'],
                                        kalman['measurement_matrix'], kalman['r']))
        self.generate_output(kalman)
        kalman['data_set'] = [kalman['data_set'][-1]]

    def generate_output(self, kalman):
        data_set = kalman['data_set']
        kalman_data_output = {
            'site': kalman['site']['name'],
            'la': kalman['site']['lat'],
            'lo': kalman['site']['lon'],
            'mn': kalman['measurement_matrix'][0, 0],
            'me': kalman['measurement_matrix'][1, 0],
            'mv': kalman['measurement_matrix'][2, 0],
            'kn': kalman['state'][0, 0],
            'ke': kalman['state'][1, 0],
            'kv': kalman['state'][2, 0],
            'cn': kalman['r'][0, 0],
            'ce': kalman['r'][1, 0],
            'cv': kalman['r'][2, 0],
            'he': kalman['site']['height'],
            'ta': kalman['tag'],
            'st': kalman['start_up'],
            'time': kalman['time'],
        }
        self.logger.debug("Kalman filter generating data for site {}, time {}".format(
            kalman['site']['name'], kalman['time']))
        kalman_out = DataStructures.get_post_kalman_queue_message(pre_kalman=data_set, kalman_data=kalman_data_output)
        self.output_queue.put((kalman_out['time_group'], kalman_out['sequence_number'], kalman_out))

    def pass_state_start(self, kalman):
        kalman['i_state'] = kalman['state'] * 1.
        kalman['i_state_2'] = kalman['state_2'] * 1.

    def pass_update_state(self, kalman):
        kalman['state'] = kalman['phi'] * kalman['state']
        kalman['state_2'] = kalman['phi'] * kalman['state_2']
        self.calc_res(kalman)

    def end_pass_state(self, kalman):
        kalman['state'] = kalman['i_state'] * 1.
        kalman['state_2]'] = kalman['i_state_2'] * 1.

    def offset_reset(self, kalman):
        kalman['state_2'] = kalman['i_state'] + kalman['i_state_2']
        kalman['state'] = np.matrix( [[0.], [0.], [0.]] )

    def eq_flag_test(self, kalman):
        return any(kalman['eq_flag'][0:3, 0])

    def eq_num_test(self, kalman):
        return max(kalman['eq_count'][0:3, 0])
