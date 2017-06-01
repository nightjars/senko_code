import DataStructures
import queue
import multiprocessing
import threading
import time as Time
import Kalman
import calendar
import logging

class DataRouter:
    # deal with the lifecycle of data going through the system
    def __init__(self, sites, faults):
        self.logger = logging.getLogger(__name__)
        self.input_data_queue = queue.Queue()
        self.data_validator_queue = queue.Queue()
        self.time_grouping_queue = queue.PriorityQueue()
        self.inverter_queue = queue.Queue()
        self.kalman_start_queue = queue.Queue()
        self.kalman_initialize_queue = queue.Queue()
        self.completed_inversion_queue = queue.Queue()

        self.kalman_map = {}
        self.kalman_lock = threading.Lock()
        self.newest_data_timestamp = 0
        self.last_sent_data_timestamp = None
        self.completed_data_count = 0
        self.terminated = False

        self.sites = sites
        self.faults = faults
        self.data_num = 0

        self.threads = []

    def router_loop(self):
        self.logger.info("Data router starting.")
        self.threads.append(threading.Thread(target=self.incoming_data_router))
        self.threads.append(threading.Thread(target=self.kalman_initializer))
        self.threads.append(threading.Thread(target=self.time_grouper))

        for thread in self.threads:
            thread.start()
        # may want to add logic to restart threads if something dies

    def incoming_data_router(self):
        while not self.terminated:
            try:
                new_data = self.input_data_queue.get(timeout=1)
                self.data_num += 1
                if new_data['gps_data']['site'] in self.kalman_map:
                    # If there's in entry in the kalman map, the kalman filter is in one of two states:
                    # activity processing, in which case add the work to the filter's queue.  If not actively
                    # processing, either re-start the kalman filter or create a new one, based on if the
                    # data has passed its defined expiration date

                    kalman = self.kalman_map[new_data['gps_data']['site']]
                    measure_time = new_data['gps_data_timestamp']
                    kalman['measurement_queue'].put((measure_time, self.data_num,
                                                     DataStructures.get_gps_data_queue_message(new_data,
                                                     kalman_verify_step=True,kalman_filter_step=True)))

                    # lock will be set if a thread is currently working on this data
                    if kalman['lock'].acquire(False):
                        # Lock acquired, nothing is processing this data
                        if kalman['last_calculation'] is not None and                                      \
                                        calendar.timegm(Time.gmtime()) - kalman['last_calculation'] >      \
                                        DataStructures.configuration['kalman_stale']:
                            self.kalman_map[new_data['gps_data']['site']] = \
                                DataStructures.get_empty_kalman_state(self.sites, self.faults)
                            kalman['lock'].release()
                            self.input_data_queue.put(new_data)
                        else:
                            kalman['lock'].release()
                            self.kalman_start_queue.put(kalman)
                else:
                    self.data_validator_queue.put(new_data)
                self.newest_data_timestamp = max(new_data['gps_data_timestamp'], self.newest_data_timestamp)
            except queue.Empty:
                pass

    def kalman_initializer(self):
        while not self.terminated:
            try:
                new_data = self.kalman_initialize_queue.get(timeout=1)
                with self.kalman_lock:
                    if not new_data['gps_data']['site'] in self.kalman_map:
                        self.kalman_map[new_data['gps_data']['site']] = \
                            DataStructures.get_empty_kalman_state(self.sites, self.faults)
                self.input_data_queue.put(new_data)
            except queue.Empty:
                pass

    def time_grouper(self):
        while not self.terminated:
            Time.sleep(.1)
            if self.last_sent_data_timestamp is not None:
                oldest_good_data = self.last_sent_data_timestamp + 1
            else:
                oldest_good_data = calendar.timegm(Time.gmtime()) - DataStructures.configuration['delay_timespan']

            if self.last_sent_data_timestamp is None:
                # If counter isn't initialized, just initialize to one second older than the oldest item
                self.last_sent_data_timestamp = oldest_good_data - 1

            # See if there is any data to send
            if self.newest_data_timestamp + DataStructures.configuration['group_timespan'] -            \
                        DataStructures.configuration['delay_timespan'] > self.last_sent_data_timestamp:
                data_to_send = []
                timestamps_to_accept = list(range(self.last_sent_data_timestamp + 1,
                                                  self.last_sent_data_timestamp + 1 +
                                                  DataStructures.configuration['group_timespan']))

                while not self.terminated:
                    try:
                        (time, segment_number, data) = self.time_grouping_queue.get(timeout=1)
                        if time in timestamps_to_accept:
                            data_to_send.append(data)
                            self.last_sent_data_timestamp = max(time, self.last_sent_data_timestamp)
                        elif time < oldest_good_data:
                            self.logger.debug("{} receieved too late and discarded ({}, but working on {})".format(
                                data, time, oldest_good_data))
                        else:
                            self.time_grouping_queue.put((time, segment_number, data))
                            break
                    except queue.Empty:
                        pass

                if len(data_to_send):
                    data_to_send = DataStructures.get_grouped_inversion_queue_message(kalman_data=data_to_send,
                                                                                      inverter_wait_queue_dequeue=True)
                    self.inverter_queue.put((timestamps_to_accept, data_to_send, self.sites, self.faults))
                    self.logger.debug("Sent timegroup {} to inverter queue.".format(
                        timestamps_to_accept))
                else:
                    self.logger.debug("No data to sent to inverter for timegroup {}".format(
                        oldest_good_data))
                    self.last_sent_data_timestamp += 1
