import threading
import DataStructures
import numpy as np
from scipy import optimize
from datetime import datetime as dt
import time
import logging
import math
import ok
import multiprocessing
import queue
import hashlib

class InverterThread(threading.Thread):
    def __init__(self, input_queue, output_queue):
        self.logger = logging.getLogger(__name__)
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.terminated = False
        threading.Thread.__init__(self)

    def run(self):
        while not self.terminated:
            try:
                (time_stamp, kalman_data, conf) = self.input_queue.get(timeout=1)
                self.logger.debug("Got a group for time {} with {} kalman sets in it".
                                  format(time_stamp, len(kalman_data['kalman_output_data'])))
                kalman_data = DataStructures.get_grouped_inversion_queue_message(prev_message=kalman_data,
                                                                                 inverter_begin=True)

                offset, sub_inputs, smooth_mat, mask = InverterConfiguration.get_config(conf)
                offset = np.copy(offset)
                sub_inputs = np.copy(sub_inputs)
                mask = np.copy(mask)
                sites = conf['sites']

                site_correlate = []

                for site, kalman_output in kalman_data['kalman_output_data'].items():
                    site_idx = sites[site]['index']
                    site_correlate.append((site_idx, sites[site]))
                    mask[site_idx * 3: site_idx * 3 + 3, 0] = 1
                    if kalman_output['ta']:
                        offset[0, site_idx * 3] = kalman_output['kn']
                        offset[0, site_idx * 3 + 1] = kalman_output['ke']
                        offset[0, site_idx * 3 + 2] = kalman_output['kv']
                    else:
                        offset[0, site_idx * 3: site_idx * 3 + 3] = 0

                site_correlate.sort(key=lambda idx: idx[0])

                sub_inputs = np.vstack([sub_inputs, smooth_mat])
                present_sub_inputs = np.take(sub_inputs, np.argwhere(mask > 0)[:, 0])

                solution = optimize.nnls(sub_inputs, offset[0])[0]

                calc_offset = sub_inputs.dot(solution)

                output = self.generate_output(solution, kalman_data['kalman_output_data'], site_correlate,
                                              calc_offset, time_stamp, conf)

                kalman_data = DataStructures.get_grouped_inversion_queue_message(prev_message=kalman_data,
                                                                                 inverter_output_data=output)
                self.output_queue.put(kalman_data)
            except queue.Empty:
                pass


    def generate_output(self, solution, kalman_data, correlate, calc_offset, time_stamp, conf):
        faults = conf['faults']
        fault_sol = []
        magnitude = 0.0
        final_calc = []
        site_data = []
        slip = []
        estimates = []

        for idx, sol in enumerate(solution):
            fault_sol.append([
                faults['subfault_list'][idx][0],
                faults['subfault_list'][idx][1],
                faults['subfault_list'][idx][2],
                faults['subfault_list'][idx][3],
                faults['subfault_list'][idx][4],
                str(faults['subfault_list'][idx][7]),
                faults['subfault_list'][idx][5],
                faults['subfault_list'][idx][6],
                str(sol),
                "0",
                sol
            ])

            magnitude += float(fault_sol[-1][6]) * float(fault_sol[-1][7]) * float(1e12) *          \
                         np.abs(float(fault_sol[-1][8]))

        magnitude *= float(3e11)

        if magnitude is not 0:
            pass

        if conf['strike_slip']:
            for x in range(int(faults['length'])):
                temp = fault_sol[x]
                temp[8] = float(temp[8])
                for y in range(int(faults['width'])):
                    temp[8] += float(fault_sol[x+y*int(faults['length'])][8])
                slip.append(temp)
        else:
            for fault in fault_sol:
                slip.append(fault)

        for idx, site in enumerate(correlate):
            _, site_info = site         # split up tuple of site_idx,site
            kalman_site = kalman_data[site_info['name']]
            final_calc.append([
                kalman_site['site'],
                kalman_site['la'],
                kalman_site['lo'],
                kalman_site['he'],
                kalman_site['kn'] if kalman_site['ta'] else 0,
                kalman_site['ke'] if kalman_site['ta'] else 0,
                kalman_site['kv'] if kalman_site['ta'] else 0,
                calc_offset[idx * 3],
                calc_offset[idx * 3 + 1],
                calc_offset[idx * 3 + 2],
            ])
            site_data.append([
                kalman_site['site'],
                kalman_site['kn'],
                kalman_site['ke'],
                kalman_site['kv'],
                kalman_site['ta'],
                kalman_site['mn'],
                kalman_site['me'],
                kalman_site['mv'],
                kalman_site['cn'],
                kalman_site['ce'],
                kalman_site['cv'],
            ])

        for calc in final_calc:
            estimates.append(calc[0:10])

        mw = "NA" if math.isclose(0, magnitude, rel_tol=conf['float_equality']) else \
            "{:.1f}".format(2/3. * np.log10(magnitude) - 10.7)
        magnitude_str = "{:.2E}".format(magnitude)

        output = {
            'time': time_stamp[-1],
            'data': site_data,
            'label': "label and model to appear" + dt.utcfromtimestamp(float(time_stamp[-1])).strftime("%Y-%m-%d %H:%M:%S %Z"),
            'slip': slip,
            'estimates': estimates,
            'Moment': magnitude_str,
            'Magnitude': mw
        }
        return output

    def stop(self):
        self.terminated = True


class InverterConfiguration:
    inverter_config = None
    generator_lock = threading.Lock()
    logger = logging.getLogger(__name__)
    cache_validator_hash = None

    @staticmethod
    def get_config(conf):
        # to do: add logic to compare and update when sites/faults changes
        with InverterConfiguration.generator_lock:
            if InverterConfiguration.inverter_config is None:
                InverterConfiguration.inverter_config = InverterConfiguration.config_generator(conf)
        return InverterConfiguration.inverter_config

    @staticmethod
    def config_generator(conf):
        start_time = time.time()
        InverterConfiguration.logger.info("Starting inverter config processing.")
        offset_count = conf['offsets_per_site']
        subfault_wid = int(conf['faults']['width'])
        subfault_len = int(conf['faults']['length'])
        strike_slip = conf['strike_slip']
        smoothing = conf['smoothing']
        corner_fix = conf['corner_fix']
        short_smoothing = conf['short_smoothing']

        offset = np.zeros((1, len(conf['sites']) * offset_count + len(conf['faults']['subfault_list'])))
        mask = np.zeros((len(conf['sites']) * offset_count + len(conf['faults']['subfault_list']), 1))

        sub_inputs = np.zeros((len(conf['sites']) * offset_count, len(conf['faults']['subfault_list'])))
        smooth_mat = np.zeros((len(conf['faults']['subfault_list']), len(conf['faults']['subfault_list'])))

        for site_key, site in conf['sites'].items():
            for fault_idx, fault in enumerate(conf['faults']['subfault_list']):
                result = ok.dc3d(fault[0], fault[1], fault[2], fault[3], fault[4], 0., fault[5], fault[6],
                                 1, 0, site['lat'], site['lon'], 0)
                sub_inputs[site['index']*3, fault_idx] = float(result[0])
                sub_inputs[site['index']*3+1, fault_idx] = float(result[1])
                sub_inputs[site['index']*3+2, fault_idx] = float(result[2])

        if smoothing:
            if short_smoothing:
                for idx, fault in enumerate(conf['faults']['subfault_list']):
                    smooth_mat[idx, idx] = 0
                    if idx > subfault_len:
                        smooth_mat[idx, idx] = -1
                        smooth_mat[idx - subfault_len][idx] = 1
                        smooth_mat[idx, idx - subfault_len] = 1
                    if idx < subfault_len * (subfault_wid - 1):
                        smooth_mat[idx, idx] -= 1
                        smooth_mat[idx + subfault_len, idx] = 1
                        smooth_mat[idx, idx + subfault_len] = 1
                    if idx % subfault_len != 0:
                        smooth_mat[idx, idx] -= 1
                        smooth_mat[idx - 1, idx] = 1
                        smooth_mat[idx, idx-1] = 1
                    if idx % subfault_len != subfault_len - 1:
                        smooth_mat[idx, idx] -= 1
                        smooth_mat[idx + 1, idx] = 1
                        smooth_mat[idx, idx + 1] = 1
            else:
                raise NotImplementedError

            if corner_fix:
                for x in range(len(conf['faults']['subfault_list'])):
                    smooth_mat[x, x] = -4
        mask[-len(conf['faults']['subfault_list']):,0] = 1
        elapsed_time = time.time() - start_time
        InverterConfiguration.logger.info("Finished inverter config processing in {} seconds.".format(elapsed_time))
        return offset, sub_inputs, smooth_mat, mask
