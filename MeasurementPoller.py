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
        self.sequence_number = 0

        self.config = DataLoader.load_data_from_text_files(sites_data_file=DataStructures.configuration['sites_file'],
                                                           faults_data_file=DataStructures.configuration['faults_file'])

    def send_measurement(self, measurement):
        self.output_queue.put(DataStructures.get_gps_measurement_queue_message(gps_data=measurement,
                                                               gps_data_sequence_number=self.sequence_number))
        self.sequence_number += 1

    def start(self):
        pass

    def stop(self):
        self.terminated = True

class RabbitMQPoller(MeasurementPoller):
    poller_instance = None

    def __init__(self, output_queue):
        super(RabbitMQPoller, self).__init__(output_queue)
        threading.Thread(target=self.rabbit_poller).start()
        RabbitMQPoller.poller_instance = self

    @staticmethod
    def message_callback(msg):
        RabbitMQPoller.poller_instance.send_measurement(json.loads(msg.body))

    def rabbit_poller(self):
        connection = amqp.Connection(
            host=DataStructures.configuration['rabbit_mq']['host'],
            userid=DataStructures.configuration['rabbit_mq']['userid'],
            password=DataStructures.configuration['rabbit_mq']['password'],
            virtual_host=DataStructures.configuration['rabbit_mq']['virtual_host'],
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
        channel.basic_consume(callback=RabbitMQPoller.message_callback,
                              queue=queue_name,
                              no_ack=True)
        print ("about to start consuming")
        while not self.terminated:
            connection.drain_events()
        connection.close()

class NonsensePoller(MeasurementPoller):
    def __init__(self, output_queue):
        super(NonsensePoller, self).__init__(output_queue)

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

class PangaAPIPoller(MeasurementPoller):
    panga_url = 'http://www.panga.org/realtime/data/api-test20170228'
    stream_list = '/streamlist'
    stream_poll = '/slice?streams={}&window={}'
    stream_string = 'CWU_fastlane'
    records_string = '/records/{}_CWU_fastlane?&fe=covariance&l={}'
    max_requests_per_poll = 100

    def __init__(self, output_queue):
        self.streams = {}
        self.stream_list = []
        super(PangaAPIPoller, self).__init__(output_queue)

    def start(self):
        start_time = calendar.timegm(time.gmtime())
        sites = self.config['sites']
        r = requests.get(PangaAPIPoller.panga_url + PangaAPIPoller.stream_list)
        json_response = r.json()
        for i in range(0, len(json_response), 2):
            if json_response[i] in sites:
                self.streams[json_response[i]] = json_response[i + 1]
                self.stream_list.append([json_response[i], start_time])
            else:
                print ("Skipping {}".format(json_response[i]))

        threading.Thread(target=self.panga_puller).start()
        MeasurementPoller.start(self)

    def panga_puller(self):
        while not self.terminated:
            s = requests.Session()
            start = time.time()
            for site in self.stream_list:
                if site[1] > 0:
                    site_info = s.get(PangaAPIPoller.panga_url +
                                      PangaAPIPoller.records_string.format(site[0], site[1]))
                    mea = site_info.json()
                    print (mea)
                    if 'error' in mea:
                        site[1] = -1
                    else:
                        for values in mea['recs']:
                            new_data = {
                                't': int(values['t']),
                                'site': site[0],
                                'cnv': float(values['cnv']),
                                'e': float(values['e']),
                                'cn': float(values['cn']),
                                'ce': float(values['ce']),
                                'n': float(values['n']),
                                'cev': float(values['cev']),
                                'cne': float(values['cne']),
                                'v': float(values['v']),
                                'cv': float(values['cv'])
                            }
                            self.send_measurement(new_data)
                            site[1] = max(site[1], new_data['t'])

class SavedMeasurementPoller(MeasurementPoller):
    def __init__(self, output_queue, input_file='./saved_gps_data/out_600_sec'):
        self.input_file = input_file
        super(SavedMeasurementPoller, self).__init__(output_queue)

    def start(self):
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
                    time.sleep(.5)
                self.send_measurement(d)


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

#save_measurements(filename=sys.argv[1], seconds_to_save=int(sys.argv[2]))
#a = RabbitMQPoller(None)
#a = PangaAPIPoller(None)
#a = SavedMeasurementPoller(None)
#a.start()