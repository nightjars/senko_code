import unittest
import QueueManager
import DataLoader
import DataStructures
import threading
import queue
import ValidatorThread
import calendar
import time

def get_data(site_list, site=1, time=1):
    return {
        't': time,
        'site': site_list[site],
        'cnv': 1,
        'e': .3,
        'cn': 0.0001,
        'ce': 0.0001,
        'n': .3,
        'cev': 2,
        'cne': 1,
        'v': .3,
        'cv': 0.0001
    }

class TestDataRouter(unittest.TestCase):
    def setUp(self):
        self.config_data = DataLoader.load_data_from_text_files(
            sites_data_file=DataStructures.configuration['sites_file'],
            faults_data_file=DataStructures.configuration['faults_file'])
        self.sites, self.faults = (self.config_data['sites'], self.config_data['faults'])
        self.site_list = list(self.config_data['sites'].keys())

    def test_validation(self):
        router = QueueManager.QueueManager(self.sites, self.faults)
        threading.Thread(target=router.incoming_data_router).start()
        data = DataStructures.get_gps_measurement_queue_message(gps_data=get_data(self.site_list))
        router.input_data_queue.put(data)
        a = None
        try:
            a = router.data_validator_queue.get(timeout=2)
        except queue.Empty:
            pass
        if not a:
            router.stop()
            self.fail("Data router did not attempt to start a new kalman filter.")
        router.data_validator_queue.put(a)
        validator = ValidatorThread.default_validator(router.data_validator_queue,
                                                      router.kalman_initialize_queue, self.config_data)
        a = None
        validator.start()
        try:
            a = router.kalman_initialize_queue.get(timeout=2)
        except queue.Empty:
            pass
        validator.stop()
        router.stop()
        if not a:
            self.fail("Validator did not put item into kalman start queue.")

    def test_fresh_kalman_start(self):
        router = QueueManager.QueueManager(self.sites, self.faults)
        threading.Thread(target=router.incoming_data_router).start()
        threading.Thread(target=router.kalman_initializer).start()
        validator = ValidatorThread.default_validator(router.data_validator_queue,
                                                      router.kalman_initialize_queue, self.config_data)
        validator.start()
        router.input_data_queue.put(DataStructures.get_gps_measurement_queue_message(gps_data=get_data(self.site_list)))
        a = None
        try:
            a = router.kalman_start_queue.get(timeout=2)
        except queue.Empty:
            pass
        validator.stop()
        router.stop()
        if not a:
            self.fail("Kalman start request on a brand new kalman filter start did not occur.")

    def test_stale_kalman_start(self):
        router = QueueManager.QueueManager(self.sites, self.faults)
        new_kalman = DataStructures.get_empty_kalman_state(self.sites, self.faults)
        router.kalman_map[self.site_list[1]] = new_kalman
        gps_msg = get_data(self.site_list, time=1000)
        new_kalman['last_calculation'] = gps_msg['t'] - DataStructures.configuration['kalman_stale'] - 1
        new_kalman['testing_marker'] = True
        threading.Thread(target=router.incoming_data_router).start()
        threading.Thread(target=router.kalman_initializer).start()
        validator = ValidatorThread.default_validator(router.data_validator_queue,
                                                      router.kalman_initialize_queue, self.config_data)
        validator.start()
        data = DataStructures.get_gps_measurement_queue_message(gps_data=gps_msg)
        router.input_data_queue.put(data)
        a = None
        try:
            a = router.kalman_start_queue.get(timeout=2)
        except queue.Empty:
            pass
        validator.stop()
        router.stop()
        if not a:
            self.fail("Kalman start request on a stale kalman filter start did not occur.")
        if 'testing_marker' in router.kalman_map[self.site_list[1]]:
            self.fail("New Kalman state was not created for a stale Kalman filter.")

    def test_running_kalman_start(self):
        router = QueueManager.QueueManager(self.sites, self.faults)
        new_kalman = DataStructures.get_empty_kalman_state(self.sites, self.faults)
        router.kalman_map[self.site_list[1]] = new_kalman
        gps_msg = get_data(self.site_list, time=1000)
        new_kalman['last_calculation'] = gps_msg['t'] - 1
        with new_kalman['lock']:
            threading.Thread(target=router.incoming_data_router).start()
            data = DataStructures.get_gps_measurement_queue_message(gps_data=gps_msg)
            router.input_data_queue.put(data)
            a = None
            try:
                a = new_kalman['measurement_queue'].get(timeout=2)
            except queue.Empty:
                pass
            router.stop()
            if not a:
                self.fail("Kalman measurement not added to a running kalman filter.")
        self.assertTrue(router.input_data_queue.empty(), "Input data queue is not empty, but should be.")
        self.assertTrue(router.data_validator_queue.empty(), "Data validator queue is not empty, but should be.")
        self.assertTrue(router.kalman_start_queue.empty(), "Kalman start queue is not empty, but should be.")

    def test_paused_kalman_start(self):
        router = QueueManager.QueueManager(self.sites, self.faults)
        new_kalman = DataStructures.get_empty_kalman_state(self.sites, self.faults)
        router.kalman_map[self.site_list[1]] = new_kalman
        gps_msg = get_data(self.site_list, time=1000)
        new_kalman['last_calculation'] = gps_msg['t'] - DataStructures.configuration['kalman_stale']
        new_kalman['testing_marker'] = True
        threading.Thread(target=router.incoming_data_router).start()
        threading.Thread(target=router.kalman_initializer).start()
        data = DataStructures.get_gps_measurement_queue_message(gps_data=gps_msg)
        router.input_data_queue.put(data)
        a = None
        try:
            a = router.kalman_start_queue.get(timeout=2)
        except queue.Empty:
            pass
        router.stop()
        if not a:
            self.fail("Kalman start request on a paused kalman filter start did not occur.")
        a = None
        try:
            a = new_kalman['measurement_queue'].get(timeout=2)
        except queue.Empty:
            pass
        if not a:
            self.fail("Kalman measurement not added to a paused kalman filter.")
        if 'testing_marker' not in router.kalman_map[self.site_list[1]]:
            self.fail("New Kalman state was created for a paused Kalman filter, but shouldn't have been.")

    def test_time_grouper(self):
        DataStructures.configuration['group_timespan'] = 1
        DataStructures.configuration['delay_timespan'] = 15

        router = QueueManager.QueueManager(self.sites, self.faults)
        threading.Thread(target=router.incoming_data_router).start()
        threading.Thread(target=router.time_grouper).start()

        output_queue = router.inverter_queue
        input_queue = router.time_grouping_queue
        now = calendar.timegm(time.gmtime())
        input_queue.put((now, 11, DataStructures.get_grouped_inversion_queue_message(
            kalman_output_data={'dummy': None},
            gps_measurement_data={'dummy': [DataStructures.get_gps_measurement_queue_message(
                gps_data=get_data(self.site_list, time=now))]}
        )))
        for x in [now + y for y in [1, 2, 3, 13, 14, 15]]:
            router.input_data_queue.put(DataStructures.get_gps_measurement_queue_message(
                gps_data=get_data(self.site_list, time=x)))
            time.sleep(1)
            a = None
            try:
                a = output_queue.get(timeout=3)
            except queue.Empty:
                pass
            if x < now + 15:
                self.assertIsNone(a, "Should not have gotten data from inverter queue, but did, for time {}.".format(x))
            else:
                self.assertIsNotNone(a, "Should have gotten data for time {} from inverter queue, but did not.".format(x))
        router.stop()


if __name__ == '__main__':
    unittest.main()
