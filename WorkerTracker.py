import DataRouter
import threading
import time
import DataStructures
import Kalman
import Inverter
import logging
import OutputGenerator

class WorkerTracker:
    def __init__(self, router):
        self.logger = logging.getLogger(__name__)
        self.kalman_threads = []
        self.inverter_threads = []
        self.router_threads = []
        self.reporter_threads = []
        self.router = router
        self.output_generator = None
        self.terminated = False
        self.tracker_thread = threading.Thread(target=self.run, args=())
        self.tracker_thread.start()

    def run(self):
        self.router_threads.append(threading.Thread(target=self.router.router_start, args=()))
        self.router_threads[-1].start()

        self.output_generator = OutputGenerator.OutputGeneratorThread(
            self.router.completed_inversion_queue)
        self.output_generator.start()

        counter = 0
        while not self.terminated:
            counter += 1
            if counter % 100 == 0:
                self.logger.info("Work status:")
                self.logger.info("Queue sizes: {} {} {} {} {}".format(
                    self.router.input_data_queue.qsize(),
                    self.router.time_grouping_queue.qsize(),
                    self.router.inverter_queue.qsize(),
                    self.router.kalman_start_queue.qsize(),
                    self.router.completed_inversion_queue.qsize()
                ))
                self.logger.info("Last sent data:  {}  Newest seen data:  {}".format(
                      self.router.last_sent_data_timestamp,
                      self.router.newest_data_timestamp))

            if self.router.kalman_start_queue.qsize() >= DataStructures.configuration['kalman_queue_threshold']:
                if len(self.kalman_threads) < DataStructures.configuration['max_kalman_threads']:
                    self.kalman_threads.append(Kalman.KalmanThread(
                        self.router.kalman_start_queue,
                        self.router.time_grouping_queue))
                    self.kalman_threads[-1].start()

            if self.router.inverter_queue.qsize() >= DataStructures.configuration['inverter_queue_threshold']:
                if len(self.inverter_threads) < DataStructures.configuration['max_inverter_threads']:
                    self.inverter_threads.append(Inverter.InverterThread(
                        self.router.inverter_queue,
                        self.router.completed_inversion_queue))
                    self.inverter_threads[-1].start()

            time.sleep(.2)

        self.logger.info("Beginning worker thread shutdown.")
        for thread in self.inverter_threads:
            thread.terminated = True
        for thread in self.inverter_threads:
            thread.join()
        self.logger.info("Inverter threads shutdown")
        for thread in self.kalman_threads:
            thread.terminated = True
        for thread in self.kalman_threads:
            thread.join()
        self.logger.info("Kalman threads shutdown")
        for thread in self.reporter_threads:
            thread.terminated = True
        for thread in self.reporter_threads:
            thread.join()
        self.logger.info("Reporter threads shutdown")
        self.logger.info("Validator threads shutdown")
        self.output_generator.terminated = True
        self.output_generator.join()
        self.logger.info("Output generator thread shutdown")
        self.logger.info("All worker treads shutdown")
