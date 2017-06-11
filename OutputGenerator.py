import threading
import logging
import json
import multiprocessing
import queue


class OutputGeneratorThread(threading.Thread):
    def __init__(self, input_queue):
        self.logger = logging.getLogger(__name__)
        self.input_queue = input_queue
        self.terminated = False
        self.completed_data_count = 0
        threading.Thread.__init__(self)

    def run(self):
        while not self.terminated:
            try:
                completed_inversion = self.input_queue.get(timeout=1)

                self.completed_data_count += 1
                # with open('./invert_output/{}.json'.format(completed_inversion['inverter_output_data']['time']), 'a') as out_file:
                #    json.dump(completed_inversion['inverter_output_data'], out_file)
                self.logger.debug("Just completed {}".format(completed_inversion['inverter_output_data']))
                self.logger.info("Output: {} sites: {}".format(len(completed_inversion['inverter_output_data']['data']),
                                                               completed_inversion['inverter_output_data']))
            except queue.Empty:
                pass
