import threading
import logging
import json
import multiprocessing
import queue
import CompareInverterResults

class OutputGeneratorThread(threading.Thread):
    def __init__(self, input_queue):
        self.logger = logging.getLogger(__name__)
        self.input_queue = input_queue
        self.terminated = False
        self.completed_data_count = 0
        #self.cir = CompareInverterResults.InverterValidator()
        #self.cir.start()
        threading.Thread.__init__(self)

    def run(self):
        while not self.terminated:
            try:
                completed_inversion = self.input_queue.get(timeout=1)

                self.completed_data_count += 1
                output_data = completed_inversion['inverter_output_data']
                data_out = []
                for d in output_data['data']:
                    if d[4]:
                        data_out.append([d[0], d[1], d[2]])
                    else:
                        data_out.append([d[0], 0., 0.])
                output = {
                    't': float(output_data['time']),
                    'tag': 'not implemented',
                    'model': 'not implemented',
                    'result': json.dumps({
                        'estimates': [[x[0], x[7], x[8]] for x in output_data['estimates']],
                        'slip': [x[8] for x in output_data['slip']],
                        'data': data_out,
                        'time': float(output_data['time']),
                        'label': output_data['label'],
                        'Mw': output_data['Magnitude'],
                        'M': output_data['Moment']
                    })
                }
                print (output)
                #self.cir.new_inverter_data(output)
                # with open('./invert_output/{}.json'.format(completed_inversion['inverter_output_data']['time']), 'a') as out_file:
                #    json.dump(completed_inversion['inverter_output_data'], out_file)
                #self.logger.debug("Just completed {}".format(completed_inversion['inverter_output_data']))
                #self.logger.info("Output: {} sites: {}".format(len(completed_inversion['inverter_output_data']['data']),
                #                                               completed_inversion['inverter_output_data']))
                #final_outputl
            except queue.Empty:
                pass
