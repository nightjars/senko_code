import DataStructures
import DataLoader
import threading
import random
import time

class MeasurementPoller:
    def __init__(self, output_queue):
        self.output_queue = output_queue
        self.terminated = False
        self.sequence_number = 0

    def send_measurement(self, measurement):
        self.output_queue.put(DataStructures.get_gps_data_queue_message(gps_data=measurement,
                                                               gps_data_sequence_number=self.sequence_number))
        self.sequence_number += 1

    def terminate(self):
        self.terminated = True


class NonsensePoller(MeasurementPoller):
    def __init__(self, output_queue):
        super(NonsensePoller, self).__init__(output_queue)
        self.config = DataLoader.load_data_from_text_files(sites_data_file=DataStructures.configuration['sites_file'],
                                                           faults_data_file=DataStructures.configuration['faults_file'])
        threading.Thread(target=self.nonsense_generator).start()

    def nonsense_generator(self):
        cur_time = 0
        site_list = list(self.config['sites'].keys())
        while not self.terminated:
            for x in range(100):
                if random.random() < .3:
                    cur_time += 1
                new_data = {
                    't': cur_time,
                    'site': site_list[random.randint(0, 10)], #len(site_list) -1)],
                    'cnv': random.random() * 2 - 1,
                    'e': random.random() * 2 - 1,
                    'cn': random.random() * 2 - 1,
                    'ce': random.random() * 2 - 1,
                    'n': random.random() * 2 - 1,
                    'cev': random.random() * 4 - 2,
                    'cne': random.random() * 2 - 1,
                    'v': random.random() * 2 - 1,
                    'cv': random.random() * 20 - 10
                }
                self.send_measurement(new_data)
            time.sleep(random.random() * 0.2)
