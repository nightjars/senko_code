import threading
import logging
import json
import multiprocessing
import queue
import CompareInverterResults
import pika
import time

class OutputGeneratorThread(threading.Thread):
    def __init__(self, input_queue):
        self.logger = logging.getLogger(__name__)
        self.input_queue = input_queue
        self.terminated = False
        self.completed_data_count = 0
        #self.cir = CompareInverterResults.InverterValidator()
        #self.cir.start()
        threading.Thread.__init__(self)

        self.exchange = 'slip-inversion2'
        self.host = "pc96225.d.cwu.edu"
        self.port = 5672
        self.userid = "nif"
        self.virtual_host = "/rtgps-products"
        self.password = "nars0add"
        self.model = "Test"

    def run(self):
        credentials = pika.PlainCredentials(self.userid, self.password)
        parameters = pika.ConnectionParameters(self.host, self.port, self.virtual_host, credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, type='topic', durable=True, auto_delete=False)

        while not self.terminated:
            try:
                (output_data, model, tag) = self.input_queue.get(timeout=1)

                self.completed_data_count += 1
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
                #print ("---")
                self.channel.basic_publish(exchange=self.exchange, routing_key=self.model, body=json.dumps(output))
                #print ("Published data for t:{}".format(output['t']))

                #self.cir.new_inverter_data(output)
                # with open('./invert_output/{}.json'.format(completed_inversion['inverter_output_data']['time']), 'a') as out_file:
                #    json.dump(completed_inversion['inverter_output_data'], out_file)
                #self.logger.debug("Just completed {}".format(completed_inversion['inverter_output_data']))
                #self.logger.info("Output: {} sites: {}".format(len(completed_inversion['inverter_output_data']['data']),
                #                                               completed_inversion['inverter_output_data']))
                #final_outputl
            except queue.Empty:
                pass
