import abc
import threading
import queue
import DataStructures
import logging
import multiprocessing
import queue


class ValidatorThread(threading.Thread):

    def __init__(self, input_queue, output_queue, data):
        self.logger = logging.getLogger(__name__)
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.data = data
        self.terminated = False
        threading.Thread.__init__(self)
        super(ValidatorThread, self).__init__()

    def stop(self):
        self.terminated = True


class PrecalculatedOffsetValidator(ValidatorThread):

    def run(self):
        calcdata = self.data['sites']
        while not self.terminated:
            try:
                new_data = self.input_queue.get(timeout=1)
                if new_data['gps_data']['site'] in calcdata:
                    # this seems to be a completely static thing, not dependent on the data coming in, is this intended?
                    if calcdata[new_data['gps_data']['site']]['minimum_offset'] >= DataStructures.configuration['minimum_offset']:
                        self.output_queue.put(DataStructures.get_gps_data_queue_message(new_data,kalman_verify_step=True))
                    else:
                        self.logger.info("Ignoring {} {}".format(new_data['gps_data']['site'], calcdata[new_data['gps_data']['site']]['minimum_offset']))
            except queue.Empty:
                pass


def default_validator(input_queue, output_queue, data):
    return PrecalculatedOffsetValidator(input_queue, output_queue, data)