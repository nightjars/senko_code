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
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.conf = DataStructures.configuration
        self.input_data_queue = queue.Queue()
        self.time_grouping_queue = queue.Queue()
        self.inverter_queue = queue.Queue()
        self.kalman_start_queue = queue.Queue()
        self.completed_inversion_queue = queue.Queue()

        self.inversion_runs = DataStructures.inversion_runs
        self.newest_data_timestamp = 0
        self.last_sent_data_timestamp = None
        self.completed_data_count = 0
        self.terminated = False

        self.data_num = 0

        self.time_grouped_messages = {}
        self.initial_time_stamp = None

        self.threads = []

    def stop(self):
        self.terminated = True

    def router_start(self):
        self.logger.info("Data router starting.")
        self.threads.append(threading.Thread(target=self.incoming_data_router))
        self.threads.append(threading.Thread(target=self.time_grouper))

        for thread in self.threads:
            thread.start()
        # may want to add logic to restart threads if something dies

    def incoming_data_router(self):
        while not self.terminated:
            try:
                new_data = self.input_data_queue.get(timeout=1)
                self.data_num += 1
                if self.initial_time_stamp is None:
                    self.initial_time_stamp = new_data['t']
                for run in self.inversion_runs:
                    if new_data['site'] not in run['filters']:
                        if new_data['site'] in run['sites']:
                            if not new_data['site'] in run['filters']:
                                run['filters'][new_data['site']] = \
                                    DataStructures.get_empty_kalman_state(run)
                    if new_data['site'] in run['filters']:
                        kalman = run['filters'][new_data['site']]
                        # The following temp_kill logic will kill the filter after a certain number of measuemrents gets ignored
                        # because of it's state/state2 varibles causing 'res' to exceed maxoffset.  This causes the filter to get
                        # stuck and preventus further input from being processed.  Once the actual problem with the kalman filter is
                        # solved, this if-block should be removed.
                        if kalman['temp_kill'] > kalman['temp_kill_limit']:
                            self.logger.info("Restarted jammed filter {}".format(kalman['site']))
                            kalman = DataStructures.get_empty_kalman_state(run)
                            run['filters'][new_data['site']] = kalman

                        measure_time = new_data['t']
                        # If there's in entry in the kalman map, the kalman filter is in one of two states:
                        # activity processing, in which case add the work to the filter's queue.  If not actively
                        # processing, re-start the kalman filter.
                        kalman['measurement_queue'].put((measure_time, self.data_num, new_data))

                        # lock will be set if a thread is currently working on this data
                        #if kalman['lock'].acquire(False):
                        #    # Lock acquired, nothing is processing this data, put into start queue
                        #    kalman['lock'].release()
                        self.kalman_start_queue.put(kalman)
                self.newest_data_timestamp = max(new_data['t'], self.newest_data_timestamp)
            except queue.Empty:
                pass

    def time_grouper(self):
        run_data = {}
        for run in DataStructures.inversion_runs:
            run_data[run['model']] = run

        while not self.terminated:
            (time, data, run) = self.time_grouping_queue.get()
            if self.last_sent_data_timestamp is None:
                self.last_sent_data_timestamp = self.initial_time_stamp - 1
            if time > self.last_sent_data_timestamp:
                if time not in self.time_grouped_messages:
                    self.time_grouped_messages[time] = {}
                if run['model'] in self.time_grouped_messages[time]:
                    self.time_grouped_messages[time][run['model']][data['site']] = data
                else:
                    self.time_grouped_messages[time][run['model']] = {data['site']: data}
            else:
                self.logger.info("Data for {} was too late {}, working on {}".format(
                    data['site'], time, self.last_sent_data_timestamp))
            if self.newest_data_timestamp - self.conf['delay_timespan'] > self.last_sent_data_timestamp:
                self.last_sent_data_timestamp += 1
                if self.last_sent_data_timestamp in self.time_grouped_messages:
                    for model, measurements in self.time_grouped_messages[self.last_sent_data_timestamp].items():
                        data_to_send = measurements
                        self.inverter_queue.put((self.last_sent_data_timestamp, data_to_send, run_data[model]))
                    self.logger.info("Sent timegroup {} to inverter queue.".format(self.last_sent_data_timestamp))
                    del self.time_grouped_messages[self.last_sent_data_timestamp]
