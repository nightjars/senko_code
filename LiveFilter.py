import DataStructures
import DataLoader
import MeasurementPoller
import queue
import DataStructures
import time
import threading
import Kalman
import QueueManager
import logging


class LiveFilter:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO, format='%(relativeCreated)6d %(threadName)s %(message)s')

        self.logger.info("LiveFilter starting.")
        DataLoader.load_data_from_text_files()
        self.queue_manager = QueueManager.QueueManager()

        # Selection for using saved measurements or get measurements from RabbitMQ server
        self.data_source = MeasurementPoller.RabbitMQPoller(self.queue_manager.input_data_queue)
        #self.data_source = MeasurementPoller.SavedMeasurementPoller(self.queue_manager.input_data_queue)

    def start(self):
        self.data_source.start()
        self.queue_manager.start()

    def stop(self):
        self.queue_manager.terminated = True
        self.data_source.terminated = True

def main():
    lv = LiveFilter()
    lv.start()

if __name__ == '__main__':
    main()
