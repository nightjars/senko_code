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
                self.logger.debug("Just completed {}".format(completed_inversion['inverter_output_data']))
                self.logger.info(
                    "Inversion for time {} completed, beginning->end: {} seconds.  Inversion included {} kalman filter outputs.".format(
                        completed_inversion['time_group'],
                        completed_inversion['timestamps']['invert_end'] -
                        completed_inversion['timestamps']['data_received'],
                        len(completed_inversion['kalman_data'])))
            except queue.Empty:
                pass