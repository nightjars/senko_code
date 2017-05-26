import abc
import threading
import queue
import DataStructures
import logging

class ValidatorThread(threading.Thread):

    def __init__(self, input_queue, output_queue, data):
        self.logger = logging.getLogger(__name__)
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.data = data
        self.terminate = False
        threading.Thread.__init__(self)
        super(ValidatorThread, self).__init__()

    def terminate(self):
        self.terminate = True


class PrecalculatedOffsetValidator(ValidatorThread):

    def run(self):
        calcdata = self.data['sites']
        while not self.terminate:
            new_data = self.input_queue.get()
            if new_data['gps_data']['site'] in calcdata:
                # this seems to be a completely static thing, not dependent on the data coming in, is this intended?
                if calcdata[new_data['gps_data']['site']]['minimum_offset'] >= DataStructures.configuration['minimum_offset']:
                    self.output_queue.put(DataStructures.get_gps_data_queue_message(new_data,kalman_verify_step=True))
                else:
                    self.logger.info("Ignoring {} {}".format(new_data['gps_data']['site'], calcdata[new_data['gps_data']['site']]['minimum_offset']))