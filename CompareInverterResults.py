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

class InverterValidator(threading.Thread):
    old_data = {}

    def __init__(self):
        self.old_inverter_connection = amqp.Connection(
            host='pc96225.d.cwu.edu',
            userid='nif_ro',
            password='ro',
            virtual_host='/rtgps-products',
            exchange='slip-inversion2'
        )
        self.terminated = False
        self.unchecked_data = []
        threading.Thread.__init__(self)

    @staticmethod
    def message_callback(msg):
        old_inverter_msg = json.loads(msg.body.decode("utf-8"))
        InverterValidator.old_data[old_inverter_msg['t']] = old_inverter_msg


    def run(self):
        connection = self.old_inverter_connection
        connection.connect()
        channel = connection.channel()
        channel.exchange_declare('slip-inversion2', 'topic', passive=True)
        queue_name = channel.queue_declare()[0]
        print (queue_name)
        channel.queue_bind(queue_name, exchange='slip-inversion2', routing_key='#')
        channel.basic_consume(callback=InverterValidator.message_callback,
                              queue=queue_name,
                              no_ack=True)
        print ("about to start consuming")
        while not self.terminated:
            connection.drain_events()
        connection.close()

    def new_inverter_data(self, data):
        if data['t'] in InverterValidator.old_data:
            self.compare(InverterValidator.old_data[data['t']], data)
            del InverterValidator.old_data[data['t']]
        else:
            self.unchecked_data.append(data)

        # very inefficient way of doing this, but this is just a temporary validation measure
        # so it will go away

        for check in self.unchecked_data:
            if check['t'] in InverterValidator.old_data:
                self.compare(InverterValidator.old_data[check['t']], check)
                self.unchecked_data.remove(check)
                del InverterValidator.old_data[check['t']]

    def compare(self, old, new):
        o = json.loads(old['result'])
        n = json.loads(new['result'])
        for x in o:
            old[x] = o[x]
        for x in n:
            new[x] = n[x]
        print (o)
        print (n)
        check_list = ['M', 'Mw', 'label', 't', 'tag', 'model']
        display_list = [ 'M', 'w', 'l', 't', 'g', 'm']
        slip_len = len(old['slip']) == len(new['slip'])
        results = []
        slip_same = None
        for item in check_list:
            results.append(old[item] == new[item])
        if slip_len:
            slip_same = True
            for idx, slip in enumerate(old['slip']):
                if slip != new['slip'][idx]:
                    slip_same = False
        else:
            slip_same = False

        out_str = ""
        for idx, result in enumerate(results):
            out_str += check_list[idx] if result else " "

        out_str += " Slips: " + str(slip_same)

        print ("t:{} results: {}".format(old['t'], out_str))

#a = InverterValidator()
#a.start()