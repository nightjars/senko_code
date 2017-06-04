import unittest
import Kalman
import old_kalman
import multiprocessing as mp
import threading
import DataStructures
import LiveFilter
import time
import logging
import queue
import random


class TestKalmanFilter(unittest.TestCase):
    old_result = None
    new_result = None
    test_run_lock = mp.Lock()

    def setUp(self):
        self.logger = logging.getLogger(__name__)
        with TestKalmanFilter.test_run_lock:
            if TestKalmanFilter.old_result is None:
                data_source = mp.Queue()
                new_kalman_output = mp.Queue()

                lf = LiveFilter.LiveFilter()
                new_data_input = lf.data_router.input_data_queue
                lf.data_source.output_queue = data_source
                lf.data_router.time_grouping_queue = new_kalman_output
                lf.start()

                #collect a few messages for a site
                data_messages = []
                while len(data_messages) < 15:
                    msg = data_source.get()
                    if len(data_messages):
                        # to simplify things, filter out all but a single site
                        if msg['gps_data']['site'] == \
                                data_messages[-1]['gps_data']['site']:
                            data_messages.append(msg)
                    else:
                        data_messages.append(msg)
                lf.stop()

                ipipe = mp.Pipe()
                opipe = mp.Pipe()
                cpipe = mp.Pipe()

                old_kal = old_kalman.Kalman()
                old_kal.setName(data_messages[-1]['gps_data']['site'])
                old_kal.InitFilter(data_messages[-1]['gps_data']['t'])
                old_kal.Init_Filter(ipipe[0], opipe[0], cpipe[0])
                old_kal_thread = threading.Thread(target=old_kal.FilterOn)
                old_kal_thread.start()

                new_kal_in = queue.Queue()
                new_kal_out = queue.Queue()
                new_kal = Kalman.KalmanThread(new_kal_in, new_kal_out)
                new_kal.force_no_delta_t = True
                new_kal.start()
                new_kal_state = DataStructures.get_empty_kalman_state(lf.data_router.sites, lf.data_router.faults)
                new_kal_state['site'] = data_messages[-1]['gps_data']['site']

                old_data_back = []
                for message in data_messages:
                    ipipe[1].send(message['gps_data'])

                time.sleep(5)

                while opipe[1].poll():
                        old_data_back.append(opipe[1].recv())


                new_data_back = []
                for message in data_messages:
                    new_kal_state['measurement_queue'].put((message['gps_data_timestamp'],
                                                            message['gps_data_sequence_number'],
                                                            message))

                if not new_kal_state['lock'].locked():
                    new_kal_in.put(new_kal_state)

                time.sleep(5)
                while not new_kal_out.empty():
                    _, _, data = new_kal_out.get(timeout=1)
                    for _, val in data['kalman_output_data'].items():
                        new_data_back.append(val)


                new_kal.terminated = True
                old_kal.run = False
                old_kal.Running = False
                TestKalmanFilter.old_result = old_data_back
                TestKalmanFilter.new_result = new_data_back

    def test_kalman_data_length(self):
        self.assertEqual(len(TestKalmanFilter.old_result), len(TestKalmanFilter.new_result),
                         "Output lengths do not match!")

    def test_kalman_data_match(self):
        self.logger.info("Comparing: {}".format(TestKalmanFilter.old_result))
        self.logger.info("With: {}".format(TestKalmanFilter.new_result))
        ignore_list=['he', 'la', 'lo']
        for idx, test_result in enumerate(TestKalmanFilter.old_result):
            for key, value in test_result.items():
                if key not in ignore_list:
                    self.assertEqual(value, TestKalmanFilter.new_result[idx][key],
                                     "Kalman results for result {} key {} do not match.".format(idx, key))

if __name__ == '__main__':
    unittest.main()
