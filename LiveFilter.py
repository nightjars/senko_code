import DataStructures
import DataLoader
import MeasurementPoller
import queue
import DataStructures
import time
import threading
import Kalman
import WorkerTracker
import DataRouter
import logging


class LiveFilter:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO, format='%(relativeCreated)6d %(threadName)s %(message)s')

        self.logger.info("LiveFilter starting.")
        self.config = DataLoader.load_data_from_text_files(sites_data_file=DataStructures.configuration['sites_file'],
                                                         faults_data_file=DataStructures.configuration['faults_file'])
        self.worker_tracker = None
        self.data_router = DataRouter.DataRouter(self.config['sites'], self.config['faults'])
        self.data_source = MeasurementPoller.NonsensePoller(self.data_router.input_data_queue)

    def start(self):
        self.worker_tracker = WorkerTracker.WorkerTracker(self.data_router, self.config)
        self.data_source.start()

    def stop(self):
        self.data_router.terminated = True
        self.worker_tracker.terminated = True
        self.data_source.terminated = True

def main():
    lv = LiveFilter()
    lv.start()

if __name__ == '__main__':
    main()