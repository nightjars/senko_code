import unittest
import Inverter
import old_inverter
import multiprocessing as mp
import threading
import DataStructures
import LiveFilter
import time
import logging

class InverterTests(unittest.TestCase):
    old_result = None
    new_result = None
    test_run_lock = mp.Lock()

    def setUp(self):
        self.logger = logging.getLogger(__name__)
        with InverterTests.test_run_lock:
            if InverterTests.old_result is None:
                data_source = mp.Queue()
                lf = LiveFilter.LiveFilter()
                lf.data_router.inverter_queue = data_source
                lf.start()

                full_inverter_msg = data_source.get()
                old_inverter_msg = full_inverter_msg[1]
                lf.stop()

                inverter_in_queue = mp.Queue()
                inverter_out_queue = mp.Queue()
                cpipe = mp.Pipe()
                ipipe = mp.Pipe()
                opipe = mp.Pipe()

                old_i = old_inverter.TVLiveSlip(ipipe[0], opipe[0], cpipe[0])
                new_i = Inverter.InverterThread(inverter_in_queue, inverter_out_queue)
                old_thread = threading.Thread(target=old_i.Run)
                new_i.start()
                old_thread.start()

                start = time.time()
                inverter_in_queue.put(full_inverter_msg)
                InverterTests.new_result = inverter_out_queue.get()['inverter_data']
                new_elapsed_time = time.time() - start

                start = time.time()
                station_data = []
                for data in old_inverter_msg['kalman_data']:
                    station_data.append([old_inverter_msg['time_group'], data['kalman_data']])
                    old_i.add(station_data[-1][1]['site'])

                ipipe[1].send(station_data)
                InverterTests.old_result = opipe[1].recv()
                old_elapsed_time = time.time() - start

                new_i.terminated = True
                self.logger.info("New inverter time to execute: {} seconds".format(new_elapsed_time))
                self.logger.info("Old inverter time to execute: {} seconds".format(old_elapsed_time))

    def test_compare_slip_output_lengths(self):
        old_slip = InverterTests.old_result['slip']
        new_slip = InverterTests.new_result['slip']
        self.assertEqual(len(old_slip), len(new_slip), "Slip output lengths don't match.")

    def test_compare_slip_data_contents(self):
        old_slip = InverterTests.old_result['slip']
        new_slip = InverterTests.new_result['slip']
        for x, slip in enumerate(old_slip):
            for y in range(len(slip)):
                self.assertEqual(float(old_slip[x][y]), float(new_slip[x][y]),
                    "Slip data at position [{},{}] doesn't match.".format(x, y))

    def test_compare_dict_data_contents(self):
        for test_compare in ['data', 'estimates']:
            old_data = InverterTests.old_result[test_compare]
            new_data = InverterTests.new_result[test_compare]
            self.assertEqual(len(old_data), len(new_data),
                             "Output lengths for {} don't match.".format(test_compare))

            data_dict = {}
            for item in old_data:
                data_dict[item[0]] = item

            for item in new_data:
                old_item = data_dict[item[0]]
                for x in range(len(item)):
                    try:
                        old_item[x] = float(old_item[x])
                    except:
                        pass
                    self.assertEqual(item[x], old_item[x], "Data for {} does not match for {} element {}.".format(
                        test_compare, item[0], x))

    def test_compare_simple_elements(self):
        for ele_compare in ['time', 'Moment', 'Magnitude']:
            self.assertEqual(InverterTests.old_result[ele_compare],
                             InverterTests.new_result[ele_compare],
                             "Data for {} does not match.".format(ele_compare))


if __name__ == '__main__':
    unittest.main()