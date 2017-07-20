import DataStructures
import DataLoader
import threading
import random
import time
import calendar
import amqp
import requests
import json
import queue
import sys


class MeasurementPoller:
    def __init__(self, output_queue):
        self.output_queue = output_queue
        self.terminated = False

    def send_measurement(self, measurement):
        self.output_queue.put(measurement)

    def start(self):
        pass

    def stop(self):
        self.terminated = True

class RabbitMQPoller(MeasurementPoller):
    def __init__(self, output_queue):
        super(RabbitMQPoller, self).__init__(output_queue)

    def start(self):
        threading.Thread(target=self.rabbit_poller).start()
        super().start()

    def rabbit_poller(self):
        def message_callback(msg):
            self.send_measurement(json.loads(msg.body))

        connection = amqp.Connection(
            host=DataStructures.configuration['rabbit_mq_input']['host'],
            userid=DataStructures.configuration['rabbit_mq_input']['userid'],
            password=DataStructures.configuration['rabbit_mq_input']['password'],
            virtual_host=DataStructures.configuration['rabbit_mq_input']['virtual_host'],
            exchange=DataStructures.configuration['rabbit_mq_input']['exchange_name']
        )
        connection.connect()
        channel = connection.channel()
        channel.exchange_declare(DataStructures.configuration['rabbit_mq_input']['exchange_name'],
                                 'test_fanout', passive=True)
        queue_name = channel.queue_declare(exclusive=True)[0]
        channel.queue_bind(queue_name, exchange=
                DataStructures.configuration['rabbit_mq_input']['exchange_name'])
        channel.basic_consume(callback=message_callback, queue=queue_name, no_ack=True)
        while not self.terminated:
            connection.drain_events()
        connection.close()


# feed saved measurements though the system for testing
class SavedMeasurementPoller(MeasurementPoller):
    def __init__(self, output_queue, input_file='./saved_gps_data/out_600_sec'):
        self.input_file = input_file
        super(SavedMeasurementPoller, self).__init__(output_queue)

    def start(self):
        threading.Thread(target=self.saved_poller).start()
        super().start()

    def saved_poller(self):
        with open(self.input_file) as f:
            data = json.load(f)

        init_time = None
        last = 0
        for d in data:
            if not self.terminated:
                if init_time is None:
                    init_time = calendar.timegm(time.gmtime()) - d['t']
                d['t'] += init_time
                if d['t'] > last:
                    last = d['t']
                    time.sleep(.8)
                self.send_measurement(d)


# code to save measurements to a file to allow later use of savedmeasurementpoller
def save_measurements(filename = 'data_output', seconds_to_save = 20):
    q = queue.Queue()
    poller = RabbitMQPoller(q)
    msg = q.get()
    data = []
    print (msg)
    first_timestamp = msg['gps_data']['t']
    current_timestamp = msg['gps_data']['t']
    while current_timestamp - first_timestamp < seconds_to_save:
        data.append(q.get()['gps_data'])
        current_timestamp = data[-1]['t']

    with open(filename, 'w') as f:
        json.dump(data, f)

    poller.stop()

# Commented-out code for use to save measurements, etc
#save_measurements(filename=sys.argv[1], seconds_to_save=int(sys.argv[2]))
#a = RabbitMQPoller(None)
#a = PangaAPIPoller(None)
#a = SavedMeasurementPoller(None)
#a.start()