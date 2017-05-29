import unittest
import Inverter
import old_inverter
import multiprocessing as mp
import threading
import DataStructures
import LiveFilter
import time

class MyTestCase(unittest.TestCase):
    def test_something(self):
        lf = LiveFilter.LiveFilter()
        lf.start()
        inverter_in_queue = mp.Queue()
        while lf.data_router is None:
            time.sleep (.5)
        lf.data_router.inverter_queue = inverter_in_queue

        inverter_msg = inverter_in_queue.get()
        print (inverter_msg)


        inverter_out_queue = mp.Queue()

        cpipe = mp.Pipe()
        ipipe = mp.Pipe()
        opipe = mp.Pipe()
        old_i = old_inverter.TVLiveSlip(cpipe[0], ipipe[0], opipe[0])
        new_i = Inverter.InverterThread(inverter_in_queue, inverter_out_queue)
        old_thread = threading.Thread(target=old_i.Run)
        new_i.start()
        old_thread.start()


        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()