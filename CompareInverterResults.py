import Config
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
import math

class InverterValidator(threading.Thread):
    old_data = {}
    me = None

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
        InverterValidator.me = self
        threading.Thread.__init__(self)
        self.cumulative = 0
        self.cumulative_total = 0.001

    @staticmethod
    def message_callback(msg):
        old_inverter_msg = json.loads(msg.body.decode("utf-8"))
        if 'TestSA' in old_inverter_msg['model']:
            InverterValidator.me.new_inverter_data(old_inverter_msg)
        elif 'SanA' in old_inverter_msg['model']:
            InverterValidator.old_data[old_inverter_msg['t']] = old_inverter_msg
            #print ("Data from old")

    def run(self):
        connection = self.old_inverter_connection
        connection.connect()
        channel = connection.channel()
        channel.exchange_declare('slip-inversion2', 'topic', passive=True)
        queue_name = channel.queue_declare()[0]
        #print (queue_name)
        channel.queue_bind(queue_name, exchange='slip-inversion2', routing_key='#')
        channel.basic_consume(callback=InverterValidator.message_callback,
                              queue=queue_name,
                              no_ack=True)
        #print ("about to start consuming")
        while not self.terminated:
            connection.drain_events()
        connection.close()

    def new_inverter_data(self, data):
        if data['t'] in InverterValidator.old_data:
            self.compare(InverterValidator.old_data[data['t']], data)
            del InverterValidator.old_data[data['t']]
        else:
            self.unchecked_data.append(data)
            #print ("adding data, none from old")

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
        #print ("Old output------------")
        #print (o)
        #print("New output------------")
        #print (n)
        #print ('-----------')       
        check_list = ['M', 'Mw', 'label', 't', 'tag', 'model']
        display_list = [ 'M', 'w', 'l', 't', 'g', 'm']
        slip_len = len(old['slip']) == len(new['slip'])
        results = []
        slip_same = None
        for item in check_list:
            results.append(old[item] == new[item])

        out_str = ""
        for idx, result in enumerate(results):
            out_str += display_list[idx] if result else " "

        diff_slip = 0
        tot_slip = 0

        if slip_len:
            for idx, slip in enumerate(old['slip']):
                diff_slip += float(slip) - float(new['slip'][idx])
                tot_slip += float(slip)
 
        #print (slip_len)
        #print (len(old['slip']))
        #print (len(new['slip']))
        self.cumulative += diff_slip
        self.cumulative_total += tot_slip

        print ("t:{} res: {} O/N: Mw {}/{}, M {}/{}, Sites {}/{} Slip diff {:.5f}% cumulative diff {:.5f}%".format(old['t'], out_str,
           old['Mw'], new['Mw'], old['M'], new['M'],  len(old['estimates']), len(new['estimates']),
           diff_slip / tot_slip * 100 if tot_slip != 0 else 0, self.cumulative / self.cumulative_total * 100))

if __name__ == "__main__":
    a = InverterValidator()
    a.start()
