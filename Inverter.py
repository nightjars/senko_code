import threading
import DataStructures
import numpy as np
from datetime import datetime as dt
import time as Time
import logging

class InverterThread(threading.Thread):
    def __init__(self, input_queue, output_queue):
        self.logger = logging.getLogger(__name__)
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.terminate = False
        threading.Thread.__init__(self)

    def run(self):
        while not self.terminate:
            (time, kalman_data, sites, faults) = self.input_queue.get()
            self.logger.info("Got a group for time {} with {} kalman sets in it".format(time, len(kalman_data['kalman_data'])))
            kalman_data = DataStructures.get_grouped_inversion_queue_message(prev_message=kalman_data,
                                                                          inverter_begin=True)

            if InverterConfiguration.inverter_config is None:
                InverterConfiguration(sites, faults)

            offset, add_matrix, sub_inputs, smooth_mat, mask = InverterConfiguration.get_config()
            offset = np.copy(offset)
            add_matrix = np.copy(add_matrix)
            sub_inputs = np.copy(sub_inputs)
            mask = np.copy(mask)

            site_correlate = []

            for kalman_output in kalman_data['kalman_data']:
                site_idx = sites[kalman_output['kalman_data']['site']]['index']
                site_correlate.append((site_idx, sites[kalman_output['kalman_data']['site']]))
                mask[site_idx * 3: site_idx * 3 + 3, 0] = 1
                if kalman_output['kalman_data']['ta']:
                    offset[0, site_idx] = kalman_output['kalman_data']['kn']
                    offset[0, site_idx + 1] = kalman_output['kalman_data']['ke']
                    offset[0, site_idx + 2] = kalman_output['kalman_data']['kv']
                else:
                    offset[0, site_idx * 3: site_idx * 3 + 3] = 0

            site_correlate.sort(key=lambda idx: idx[0])

            sub_inputs = np.vstack([sub_inputs, smooth_mat])
            present_sub_inputs = np.take(sub_inputs, np.argwhere(mask > 0)[:, 0])

            # nnls

            solution = np.copy(sub_inputs)  # need scipy
            calc_offset = np.copy(solution) #sub_inputs.dot(solution)

            output = self.generate_output(solution, kalman_data['kalman_data'], site_correlate,
                                          calc_offset, sites, faults)

            kalman_data = DataStructures.get_grouped_inversion_queue_message(prev_message=kalman_data,
                                                                             inverter_data="inverter output")
            self.output_queue.put(kalman_data)

    def generate_output(self, solution, kalman_data, correlate, calc_offset, sites, faults):
        fault_sol = []
        magnitude = 0.0
        final_calc = []
        site_data = []
        time = kalman_data[0]['kalman_data']['time']
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

            #magnitude += float(fault_sol[-1][6]) * float(fault_sol[-1][7]) * float(1e12) *          \
            #             np.abs(float(fault_sol[-1][8]))

        magnitude *= float(3e11)

        if magnitude is not 0:
            pass

        if DataStructures.configuration['strike_slip']:
            raise NotImplementedError
        else:
            for x in range(int(faults['length'])):
                temp = fault_sol[x]
                temp[8] = float(temp[8])
                for y in range(int(faults['width'])):
                    temp[8] += float(fault_sol[x+y*int(faults['length'])][8])
                slip.append(temp)

        for idx, site in enumerate(correlate):
            kalman_site = kalman_data[site['name']]
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

        mw = "NA" if magnitude is 0 else "{:.1f}".format(2/3. * np.log10(magnitude) - 10.7)
        magnitude_str = "{:.2E}".format(magnitude)

        output = {
            'time': time,
            'data': site_data,
            'label': "label and model to appear" + dt.utcfromtimestamp(float(time)).strftime("%Y-%m-%d %H:%M:%S %Z"),
            'slip': slip,
            'estimates': estimates,
            'Moment': magnitude_str,
            'Magnitude': mw
        }
    def terminate(self):
        self.terminate = True


class InverterConfiguration:
    class __InverterConfiguration:
        def __init__(self, config):
            self.config = config
    inverter_config = None

    def __init__(self, sites, faults):
        new_config = self.config_generator(sites, faults)
        if not InverterConfiguration.inverter_config:
            InverterConfiguration.inverter_config = InverterConfiguration.__InverterConfiguration(new_config)
        else:
            InverterConfiguration.__InverterConfiguration.config = new_config

    @staticmethod
    def get_config():
        return InverterConfiguration.inverter_config.config

    def config_generator(self, sites, faults):
        offset_count = DataStructures.configuration['offsets_per_site']
        subfault_wid = int(faults['width'])
        subfault_len = int(faults['length'])
        strike_slip = DataStructures.configuration['strike_slip']
        smoothing = DataStructures.configuration['smoothing']
        corner_fix = DataStructures.configuration['corner_fix']
        short_smoothing = DataStructures.configuration['short_smoothing']

        offset = np.zeros((1, len(sites) * offset_count + len(faults['subfault_list'])))
        mask = np.zeros((len(sites) * offset_count + len(faults['subfault_list']), 1))

        add_matrix = np.zeros((subfault_len, len(faults['subfault_list'])))
        sub_inputs = np.zeros((len(sites) * offset_count, len(faults['subfault_list'])))
        smooth_mat = np.zeros((len(faults['subfault_list']), len(faults['subfault_list'])))

        for site_key, site in sites.items():
            for fault_idx, fault in enumerate(faults['subfault_list']):
                result = dc3d(fault[0], fault[1], fault[2], fault[3], fault[4], 0., fault[5], fault[6],
                                 1, 0, site['lat'], site['lon'], 0)
                sub_inputs[site['index']*3, fault_idx] = float(result[0])
                sub_inputs[site['index']*3+1, fault_idx] = float(result[1])
                sub_inputs[site['index']*3+2, fault_idx] = float(result[2])

        if smoothing:
            if short_smoothing:
                for idx, fault in enumerate(faults['subfault_list']):
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
                for x in range(len(faults['subfault_list'])):
                    smooth_mat[x, x] = -4
        mask[-len(faults['subfault_list']):,0] = 1

        return offset, add_matrix, sub_inputs, smooth_mat, mask

def dc3d(a,b,c,d,e,f,g,h,i,j,k,l,m):
    return (1,2,3)
