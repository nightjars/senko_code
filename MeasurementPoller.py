import DataStructures
import DataLoader
import threading
import random
import time
import calendar
import amqp

class MeasurementPoller:
    def __init__(self, output_queue):
        self.output_queue = output_queue
        self.terminated = False
        self.sequence_number = 0

    def send_measurement(self, measurement):
        self.output_queue.put(DataStructures.get_gps_data_queue_message(gps_data=measurement,
                                                               gps_data_sequence_number=self.sequence_number))
        self.sequence_number += 1

    def start(self):
        pass

    def stop(self):
        self.terminated = True

class RabbitMQPoller(MeasurementPoller):

    def __init__(self, output_queue):
        super(RabbitMQPoller, self).__init__(output_queue)
        threading.Thread(target=self.rabbit_poller).start()

    @staticmethod
    def message_callback(ch, method, properties, body):
        print ("callback")
        print (body)

    def rabbit_poller(self):
        connection = amqp.Connection(
            host=DataStructures.configuration['rabbit_mq']['host'],
            userid=DataStructures.configuration['rabbit_mq']['userid'],
            password=DataStructures.configuration['rabbit_mq']['password'],
            virtual_host=None, #DataStructures.configuration['rabbit_mq']['virtual_host'],
            exchange=DataStructures.configuration['rabbit_mq']['exchange_name']
        )
        print ("about to connect")
        connection.connect()
        print("connected")
        channel = connection.channel()
        channel.exchange_declare(DataStructures.configuration['rabbit_mq']['exchange_name'],
                                 'test_fanout', passive=True)
        queue_name = channel.queue_declare(exclusive=True)[0]
        channel.queue_bind(queue_name, exchange=DataStructures.configuration['rabbit_mq']['exchange_name'])
        channel.basic_consume(RabbitMQPoller.message_callback, queue=DataStructures.configuration['rabbit_mq']['exchange_name'])
        print ("about to start consuming")
        channel.start_consuming()

class NonsensePoller(MeasurementPoller):
    def __init__(self, output_queue):
        super(NonsensePoller, self).__init__(output_queue)
        self.config = DataLoader.load_data_from_text_files(sites_data_file=DataStructures.configuration['sites_file'],
                                                           faults_data_file=DataStructures.configuration['faults_file'])

    def start(self):
        threading.Thread(target=self.nonsense_generator).start()
        MeasurementPoller.start(self)

    def nonsense_generator(self):
        site_list = list(self.config['sites'].keys())
        while not self.terminated:
            for x in range(100):
                if not self.terminated:
                    cur_time = calendar.timegm(time.gmtime())
                    new_data = {
                        't': cur_time,
                        'site': site_list[random.randint(0, len(site_list) - 1)],
                        'cnv': random.random() * 2 - 1,
                        'e': .3 + random.random() * .1 - .05,
                        'cn': 0.0001 + random.random() * .002 - .001,
                        'ce': 0.0001 + random.random() * .002 - .001,
                        'n': .3 + random.random() * .1 - .05,
                        'cev': random.random() * 4 - 2,
                        'cne': random.random() * 2 - 1,
                        'v': .3 + random.random() * .1 - .05,
                        'cv': 0.0001 + random.random() * .002 - .001
                    }
                    self.send_measurement(new_data)
            time.sleep(random.random() * 0.2)

#a = RabbitMQPoller(None)