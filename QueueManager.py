import Config
import queue
import multiprocessing
import threading
import time
import Kalman
import calendar
import logging
import Inverter
import pika
import json
import pymongo
import RestAPI

class QueueManager:
    # deal with the lifecycle of data going through the system
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.conf = Config.configuration
        self.input_data_queue = queue.Queue()
        self.time_grouping_queue = queue.Queue()
        self.inverter_queue = queue.Queue()
        self.kalman_start_queue = queue.Queue()
        self.completed_inversion_queue = queue.Queue()

        self.inversion_runs = Config.inversion_runs
        self.newest_data_timestamp = 0
        self.last_sent_data_timestamp = None
        self.completed_data_count = 0
        self.terminated = False
        self.restart_output_generator = False

        self.data_num = 0

        self.time_grouped_messages = {}
        self.initial_time_stamp = None

        self.threads = []
        self.inverter_threads = []
        self.kalman_threads = []


    def stop(self):
        self.terminated = True
        self.logger.info("Beginning worker thread shutdown.")
        for thread in (self.threads + self.inverter_threads + self.kalman_threads):
            thread.stop()
            thread.join()
        self.logger.info("Worker threads shutdown.")

    def start(self):
        self.logger.info("Data router starting.")
        self.threads.append(threading.Thread(target=self.incoming_data_router))
        self.threads.append(threading.Thread(target=self.time_grouper))
        self.threads.append(threading.Thread(target=self.output_generator))
        self.threads.append(threading.Thread(target=RestAPI.app.run, kwargs={'port': 5002}))
        for thread in self.threads:
            thread.start()
        while len(self.inverter_threads) < Config.configuration['max_inverter_threads']:
            self.inverter_threads.append(Inverter.InverterThread(
                self.inverter_queue,
                self.completed_inversion_queue))
            self.inverter_threads[-1].start()
        while len(self.kalman_threads) < Config.configuration['max_kalman_threads']:
            self.kalman_threads.append(Kalman.KalmanThread(
                self.kalman_start_queue,
                self.time_grouping_queue))
            self.kalman_threads[-1].start()

    def incoming_data_router(self):
        while not self.terminated:
            try:
                new_data = self.input_data_queue.get(timeout=1)
                self.data_num += 1
                if self.initial_time_stamp is None:
                    self.initial_time_stamp = new_data['t']

                for run in self.inversion_runs:
                    # Find inversion runs that need to process this measurement
                    if new_data['site'] not in run['filters']:
                        if new_data['site'] in run['sites']:
                            if not new_data['site'] in run['filters']:
                                run['filters'][new_data['site']] = \
                                    Config.get_empty_kalman_state(run)
                    if new_data['site'] in run['filters']:
                        kalman = run['filters'][new_data['site']]
                        # The following temp_kill logic will kill the filter after a certain number of measuemrents gets ignored
                        # because of it's state/state2 varibles causing 'res' to exceed maxoffset.  This causes the filter to get
                        # stuck and preventus further input from being processed.  Once the actual problem with the kalman filter is
                        # solved, this if-block should be removed.
                        if kalman['temp_kill'] > kalman['temp_kill_limit']:
                            self.logger.info("Restarted jammed filter {}".format(kalman['site']))
                            kalman = Config.get_empty_kalman_state(run)
                            run['filters'][new_data['site']] = kalman

                        measure_time = new_data['t']
                        # Place new measurement into kalman filter's measurement queue and place
                        # the filter into the start queue so that a kalman worker thread will
                        # pick it up.
                        kalman['measurement_queue'].put((measure_time, self.data_num, new_data))
                        self.kalman_start_queue.put(kalman)
                self.newest_data_timestamp = max(new_data['t'], self.newest_data_timestamp)
            except queue.Empty:
                pass

    def time_grouper(self):
        run_data = {}
        for run in Config.inversion_runs:
            run_data[run['model']] = run

        while not self.terminated:
            (timestamp, data, run) = self.time_grouping_queue.get()
            if self.last_sent_data_timestamp is None:
                self.last_sent_data_timestamp = self.initial_time_stamp - 1
            if timestamp > self.last_sent_data_timestamp:
                if timestamp not in self.time_grouped_messages:
                    self.time_grouped_messages[timestamp] = {}
                if run['model'] in self.time_grouped_messages[timestamp]:
                    self.time_grouped_messages[timestamp][run['model']][data['site']] = data
                else:
                    self.time_grouped_messages[timestamp][run['model']] = {data['site']: data}
            else:
                self.logger.info("Data for {} was too late {}, working on {}".format(
                    data['site'], timestamp, self.last_sent_data_timestamp))
            if self.newest_data_timestamp - self.conf['delay_timespan'] > self.last_sent_data_timestamp:
                self.last_sent_data_timestamp += 1
                if self.last_sent_data_timestamp in self.time_grouped_messages:
                    for model, measurements in self.time_grouped_messages[self.last_sent_data_timestamp].items():
                        data_to_send = measurements
                        self.inverter_queue.put((self.last_sent_data_timestamp, data_to_send, run_data[model]))
                    self.logger.info("Sent timegroup {} to inverter queue.".format(self.last_sent_data_timestamp))
                    del self.time_grouped_messages[self.last_sent_data_timestamp]

    def output_generator(self):
        while not self.terminated:
            self.restart_output_generator = False
            if Config.configuration['output_enabled']:
                credentials = pika.PlainCredentials(Config.configuration['rabbit_mq_output']['userid'],
                                                    Config.configuration['rabbit_mq_output']['password'])
                parameters = pika.ConnectionParameters(Config.configuration['rabbit_mq_output']['host'],
                                                       Config.configuration['rabbit_mq_output']['port'],
                                                       Config.configuration['rabbit_mq_output']['virtual_host'],
                                                       credentials)
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                channel.exchange_declare(exchange=Config.configuration['rabbit_mq_output']['exchange_name'],
                                         type='topic', durable=True, auto_delete=False)

                mongo_client = pymongo.MongoClient(Config.configuration['mongo_db_output']['host'],
                                                   Config.configuration['mongo_db_output']['port'])
                odb = mongo_client.products
                odb.authenticate(Config.configuration['mongo_db_output']['userid'],
                                 Config.configuration['mongo_db_output']['password'])
                mongo_collection = odb.slip_inversion
                while not self.terminated and not self.restart_output_generator:
                    try:
                        (output_data, model, tag) = self.completed_inversion_queue.get(timeout=1)
                        data_out = []
                        for d in output_data['data']:
                            if d[4]:
                                data_out.append([d[0], d[1], d[2]])
                            else:
                                data_out.append([d[0], 0., 0.])
                        output = {
                            't': float(output_data['time']),
                            'tag': tag,
                            'model': model,
                            'result': json.dumps({
                                'estimates': [[x[0], x[7], x[8]] for x in output_data['estimates']],
                                'slip': [x[8] for x in output_data['slip']],
                                'data': data_out,
                                'time': float(output_data['time']),
                                'label': output_data['label'],
                                'Mw': output_data['Magnitude'],
                                'M': output_data['Moment']
                            })
                        }
                        channel.basic_publish(exchange=Config.configuration['rabbit_mq_output']['exchange_name'],
                                              routing_key=Config.configuration['rabbit_mq_output']['model'],
                                              body=json.dumps(output))
                        self.logger.info(
                            "Published data to RabbitMQ server for model {} timestamp {}.".format(output['model'], output['t']))
                        mongo_collection.insert(output)
                        self.logger.info(
                            "Published data to MongoDB server for model {} timestamp {}.".format(output['model'], output['t']))
                    except queue.Empty:
                        pass
            else:
                while not self.terminated:
                    try:
                        (output_data, model, tag) = self.completed_inversion_queue.get(timeout=1)
                    except queue.Empty:
                        pass
