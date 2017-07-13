import numpy as np
import calendar
import time
import json
import DataStructures
import copy
from datetime import datetime as dt
from datetime import timedelta as td
import sys
import threading
import amqp
import DataLoader

class SavedMeasurementPoller:
    def __init__(self, input_file='./saved_gps_data/out_600_sec'):
        self.input_file = input_file
        self.measurements = {}
        self.lowest = 0
        self.highest = 0
        self.delay = 2

    def start(self):
        with open(self.input_file) as f:
            data = json.load(f)

        init_time = None
        last = 0
        measurements = {}
        for d in data:
            if init_time is None:
                init_time = calendar.timegm(time.gmtime()) - d['t']
            d['t'] += init_time
            if d['t'] > last:
                last = d['t']
            if self.lowest == 0:
                self.lowest = d['t']
            self.highest = max(d['t'], self.highest)
            if d['t'] >= self.lowest:
                if d['t'] in self.measurements:
                    self.measurements[d['t']].append(d)
                else:
                    self.measurements[d['t']] = [d]

    def get_next(self):
        send = None
        while send is None and self.highest - self.delay >= self.lowest:
            if self.lowest in self.measurements:
                send = self.measurements[self.lowest]
                del(self.measurements[self.lowest])
            self.lowest += 1
        return send


class RabbitMQPoller:
    def __init__(self):
        self.terminated = False
        self.measurements = {}
        self.lowest = 0
        self.highest = 0
        self.delay = 15
        threading.Thread(target=self.rabbit_poller).start()

    def rabbit_poller(self):
        def message_callback(msg):
            d = json.loads(msg.body)
            if self.lowest == 0:
                self.lowest = d['t']
            self.highest = max(d['t'], self.highest)
            if d['t'] >= self.lowest:
                if d['t'] in self.measurements:
                    self.measurements[d['t']].append(d)
                else:
                    self.measurements[d['t']] = [d]

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
        channel.basic_consume(callback=message_callback,
                              queue=queue_name,
                              no_ack=True)
        print ("about to start consuming")
        while not self.terminated:
            connection.drain_events()
        connection.close()

    def get_next(self):
        send = None
        while send is None and self.highest - self.delay >= self.lowest:
            if self.lowest in self.measurements:
                send = self.measurements[self.lowest]
                del (self.measurements[self.lowest])
            self.lowest += 1
        return send

class NewKalman:
    def __init__(self):
        self.outputs = None
        self.terminated = False
        self.force_no_delta_t = True
        ''' force_no_delta_t for unit tests to usefully compare output with other Kalman filter
            delta_t does not calculate time differential in old code, so this
            is needed for comparison consistency'''

    def run(self, measurement, kalman):
        np.set_printoptions(precision=30)
        self.send = None
        kalman['data_set'].append(measurement)
        self.process_measurement(kalman)
        return self.send

    def process_measurement(self, kalman):
        if kalman['last_calculation'] is None or kalman['data_set'][-1]['t'] > kalman['last_calculation']:
            gps_data = kalman['data_set'][-1]
            kalman['time'] = gps_data['t']
            kalman['prev_time'] = gps_data['t']
            if gps_data['n'] != 0. or gps_data['e'] != 0. or gps_data['v'] != 0.:
                cn = gps_data['cn']
                cv = gps_data['cv']
                ce = gps_data['ce']
                n = gps_data['n'] + kalman['synth_n']
                e = gps_data['e'] + kalman['synth_e']
                v = gps_data['v'] + kalman['synth_v']
                r = np.matrix([[cn, 0., 0.], [0., ce, 0.], [0., 0., cv]])
                measure_matrix = np.matrix([[n], [e], [v]])
                res = measure_matrix - kalman['h'] * kalman['phi'] * kalman['state'] - \
                      kalman['h'] * kalman['phi'] * kalman['state_2']
                if np.abs(res[0, 0]) < kalman['max_offset'] and np.abs(res[1, 0]) < kalman['max_offset'] and \
                                np.abs(res[2, 0]) < kalman['max_offset']:
                    if kalman['last_calculation'] is None:
                        kalman['site'] = kalman['data_set'][-1]['site']
                        kalman['state_2'] = measure_matrix * 1.0
                    else:
                        kalman['measurement_matrix'] = measure_matrix
                        r[0, 0] = max(r[0, 0], kalman['def_r'])
                        r[1, 1] = max(r[1, 1], kalman['def_r'])
                        r[2, 2] = max(r[2, 2], kalman['def_r'])
                        kalman['r'] = r
                        if kalman['offset']:
                            self.pass_update_state(kalman)
                        else:
                            self.update_matrix(kalman)
                kalman['last_calculation'] = kalman['data_set'][-1]['t']

    def update_matrix(self, kalman):
        self.output_state(kalman, print, "New Kal BUM")
        if kalman['prev_time'] is not None:
            kalman['delta_t'] = kalman['time'] - kalman['prev_time']
            kalman['prev_time'] = kalman['time']
            kalman['last_calculation'] = kalman['time']
            kalman['q'] = np.matrix([[kalman['delta_t'], 0., 0.],
                                     [0., kalman['delta_t'], 0.],
                                     [0., 0., kalman['delta_t']]])
        kalman['m'] = kalman['phi'] * kalman['p'] * kalman['phi'].T + kalman['q']
        interm = (kalman['h'] * kalman['m'] * kalman['h'].T + kalman['r']).I
        kalman['k'] = kalman['m'] * kalman['h'].T * interm
        kalman['p'] = (kalman['iden'] - kalman['k'] * kalman['h']) * kalman['m']
        self.output_state(kalman, print, "New Kal AUM")
        self.calc_res(kalman)

    def calc_res(self, kalman):
        kalman['res'] = kalman['measurement_matrix'] - kalman['h'] * kalman['phi'] * kalman['state'] - \
                        kalman['h'] * kalman['phi'] * kalman['state_2']
        if not kalman['override_flag']:
            self.determine_state(kalman)

    def determine_state(self, kalman):
        self.output_state(kalman, print, "New Kal DS")
        if kalman['sm_count'] >= kalman['smoothing'] and kalman['start_up']:
            kalman['start_up'] = False
        if kalman['sm_count'] < kalman['smoothing']:
            kalman['eq_count'] = np.matrix([[0], [0], [0]])
            self.normal_mode(kalman)
            self.end_proc(kalman)
        else:
            eq_flag = kalman['eq_flag']
            eq_count = kalman['eq_count']
            res = kalman['res']
            r = kalman['r']
            if np.abs(kalman['res'][0, 0]) < np.sqrt(kalman['r'][0, 0]) * kalman['eq_threshold']:
                kalman['eq_flag'][0, 0] = False
                kalman['eq_count'][0, 0] = 0
            else:
                kalman['eq_flag'][0, 0] = True
                kalman['eq_count'][0, 0] += 1
            if np.abs(kalman['res'][1, 0]) < np.sqrt(kalman['r'][1, 1]) * kalman['eq_threshold']:
                kalman['eq_flag'][1, 0] = False
                kalman['eq_count'][1, 0] = 0
            else:
                kalman['eq_flag'][1, 0] = True
                kalman['eq_count'][1, 0] += 1
            if np.abs(kalman['res'][2, 0]) < np.sqrt(kalman['r'][2, 2]) * kalman['eq_threshold']:
                kalman['eq_flag'][2, 0] = False
                kalman['eq_count'][2, 0] = 0
            else:
                kalman['eq_flag'][2, 0] = True
                kalman['eq_count'][2, 0] += 1
            if self.eq_flag_test(kalman) and self.eq_num_test(kalman) > kalman['wait'] and kalman['offset']:
                self.eq_state(kalman)
            elif kalman['offset'] and not self.eq_flag_test(kalman):
                self.false_eq_state(kalman)
            elif self.eq_flag_test(kalman) and not kalman['offset']:
                self.begin_eq_test_state(kalman)
            else:
                self.normal_mode(kalman)
                self.end_proc(kalman)

    def normal_mode(self, kalman):
        self.output_state(kalman, print, "New Kal NM")
        kalman['state'] = kalman['phi'] * kalman['state'] + kalman['k'] * kalman['res']
        kalman['state_2'] = kalman['phi'] * kalman['state_2']
        kalman['tag'] = True if kalman['sm_count'] < kalman['smoothing'] and not kalman['start_up'] \
            else False
        self.outputs = copy.copy(kalman)
        # self.send stuff in here, not sure if needed

    def eq_state(self, kalman):
        self.output_state(kalman, print, "New Kal PEQS")
        self.offset_reset(kalman)
        kalman['sm_count'] = 0
        kalman['s_measure'].append((kalman['data_set'][-1]['t'],
                                    kalman['measurement_matrix'], kalman['r']))
        kalman['init_p'] = kalman['p'][0, 0]
        kalman['p'] = kalman['reset_p'] * 1.0
        kalman['p_count'] = 0.
        kalman['offset'] = False
        kalman['override_flag'] = True
        for x in kalman['s_measure'][:-1]:
            kalman['time'], kalman['measurement_matrix'], kalman['r'] = x
            self.update_matrix(kalman)
            self.normal_mode(kalman)
        kalman['time'], kalman['measurement_matrix'], kalman['r'] = kalman['s_measure'][-1]
        kalman['s_measure'] = []
        kalman['write'] = True
        kalman['override_flag'] = False
        self.output_state(kalman, print, "New Kal AEQS")

    def false_eq_state(self, kalman):
        self.output_state(kalman, print, "New Kal FEQS")
        kalman['write'] = True
        self.end_pass_state(kalman)
        kalman['override_flag'] = True
        kalman['s_measure'].append((kalman['data_set'][-1]['t'],
                                    kalman['measurement_matrix'], kalman['r']))
        for x in kalman['s_measure'][:-1]:
            kalman['time'], kalman['measurement_matrix'], kalman['r'] = x
            self.calc_res(kalman)
            self.normal_mode(kalman)
            self.update_matrix(kalman)
        kalman['time'], kalman['measurement_matrix'], kalman['r'] = kalman['s_measure'][-1]
        kalman['s_measure'] = []
        kalman['offset'] = False
        kalman['override_flag'] = False

    def begin_eq_test_state(self, kalman):
        kalman['offset'] = True
        self.pass_state_start(kalman)
        kalman['write'] = False

    def end_proc(self, kalman):
        if not kalman['offset']:
            kalman['sm_count'] += 1
            self.normal_mode(kalman)
        else:
            kalman['s_measure'].append((kalman['data_set'][-1]['t'],
                                        kalman['measurement_matrix'], kalman['r']))
        self.generate_output(kalman)
        kalman['data_set'] = [kalman['data_set'][-1]]

    def generate_output(self, kalman):
        data_set = kalman['data_set']
        kalman_data_output = {
            'site': self.outputs['site'],
            'la': -120,
            'lo': 48,
            'mn': self.outputs['measurement_matrix'][0, 0],
            'me': self.outputs['measurement_matrix'][1, 0],
            'mv': self.outputs['measurement_matrix'][2, 0],
            'kn': self.outputs['state'][0, 0],
            'ke': self.outputs['state'][1, 0],
            'kv': self.outputs['state'][2, 0],
            'cn': self.outputs['r'][0, 0],
            'ce': self.outputs['r'][1, 1],
            'cv': self.outputs['r'][2, 2],
            'he': 0.0,  # kalman['site']['height'],  Old Kalman filter uses 0, should this be so?
            'ta': self.outputs['tag'],
            'st': self.outputs['start_up'],
            'time': self.outputs['time'],
        }
        self.send = kalman_data_output
        self.outputs = None

    def pass_state_start(self, kalman):
        kalman['i_state'] = kalman['state'] * 1.
        kalman['i_state_2'] = kalman['state_2'] * 1.

    def pass_update_state(self, kalman):
        kalman['state'] = kalman['phi'] * kalman['state']
        kalman['state_2'] = kalman['phi'] * kalman['state_2']
        self.calc_res(kalman)

    def end_pass_state(self, kalman):
        kalman['state'] = kalman['i_state'] * 1.
        kalman['state_2'] = kalman['i_state_2'] * 1.

    def offset_reset(self, kalman):
        kalman['state_2'] = kalman['i_state'] + kalman['i_state_2']
        kalman['state'] = np.matrix([[0.], [0.], [0.]])

    def eq_flag_test(self, kalman):
        return any(kalman['eq_flag'][0:3, 0])

    def eq_num_test(self, kalman):
        return max(kalman['eq_count'][0:3, 0])

    def output_state(self, kalman, output, message):
        return
        disp_list = ['h', 'phi', 'state', 'state_2', 'offset', 'max_offset', 'iden', 'k', 'm', 'p',
                     'measurement_matrix', 'r', 'def_r', 'synth_n', 'synth_e', 'synth_v',
                     'i_state', 'i_state_2', 'q', 'res', 'override_flag', 'sm_count',
                     'smoothing', 'start_up', 'eq_count', 'eq_flag', 'eq_threshold', 'tag',
                     's_measure', 'init_p', 'reset_p', 'p_count', 'time', 'write', 'wait']
        #output("Kalman state dump: {}".format(message))
        a = ""
        for state in disp_list:
            if state in ['smoothing', 'sm_count']:
                a += ("{} {}".format(state, float(kalman[state])))
            else:
                a += ("{} {}".format(state, kalman[state]))
        print(a)


class OldKalman:
    def __init__(self):
        #logging.info("initiating a Kalman process")
        self.NAME = ''
        self.LAT = 0.
        self.LON = 0.
        self.HEI = 0.
        self.delta_T = 1
        self.H = np.matrix([[1., 0., 0.], [0., 1., 0.], [0., 0., 1.]])
        self.iden = np.matrix([[1., 0., 0.], [0., 1., 0.], [0., 0., 1.]])
        self.K = np.matrix([[0., 0., 0.], [0., 0., 0.], [0., 0., 0.]])
        self.M = np.matrix([[0., 0., 0.], [0., 0., 0.], [0., 0., 0.]])
        self.Mea = np.matrix([[0.], [0.], [0.]])
        self.P = np.matrix([[1000., 0., 0.], [0., 1000., 0.], [0., 0., 1000.]])
        self.ResetP = self.P * 1.
        self.Phi = np.matrix([[1., 0., 0.], [0., 1., 0.], [0., 0., 1.]])
        self.Q = np.matrix([[self.delta_T, 0., 0.], [0., self.delta_T, 0.], [0., 0., self.delta_T]])
        self.R = np.matrix([[0., 0., 0.], [0., 0., 0.], [0., 0., 0.]])
        self.Res = np.matrix([[0.], [0.], [0.]])
        self.State = np.matrix([[0.], [0.], [0.]])
        self.State2 = np.matrix([[0.], [0.], [0.]])
        self.IState = np.matrix([[0.], [0.], [0.]])
        self.IState2 = np.matrix([[0.], [0.], [0.]])
        self.SMea = []
        self.DispWork = False
        self.DispInit = False
        self.DispNum = False
        self.EQPrint = False
        self.offset = False
        self.Rcount = 0
        self.InitP = 0
        self.Pcount = 0.
        self.smoothing = 60.
        self.SMCount = 0.
        self.Wait = 2
        self.EQFlag = np.matrix([[False], [False], [False]])
        self.EQDState = 0
        self.EQCount = np.matrix([[0], [0], [0]])
        self.EQThres = 0.001
        self.StateData = []
        self.Time = 0
        self.OverrideFlag = False
        self.Ready = True
        self.send = {}
        self.prevTime = 0.
        self.write = True
        self.Tag = False
        self.StartUp = True
        self.defR = 0.0001
        self.Running = False
        self.streams = []
        self.urlst = "http://www.panga.org/realtime/data/api/"
        self.urlen = "?q=5min&l="
        self.lasttime = 0
        self.clusters = []
        self.Live = False
        self.First = False
        self.ptime = 0.
        self.curtime = 0.
        self.streamtime = dt(year=1970, month=1, day=1, hour=0, minute=0, second=0)
        self.child_conn = ""
        self.First_mea = True
        self.lock = ''
        self.run = True
        self.laMea = 0
        self.DieTime = 300.
        self.PCount = 0.
        self.KillMe = False
        self.Synth = [0., 0., 0.]
        self.MaxOffset = 25.0
        self.LAT = -120
        self.LON = 48
        self.Live = True
        self.First = True
        self.lasttime = dt.now()
        #self.lock = lo
        #self.child_conn = conn
        self.Running = True
        self.ptime = dt.now()
        self.curtime = dt.now()


    def setName(self, n):
        self.NAME = str(n)

    # print "Site = " + str( self.NAME )

    def getData(self, measurement):
        #logging.debug("Starting Kalman_getData at time: {}".format(dt.now()))
        self.output = None
        self.NAME = measurement['site']
        update = False
        num = 0
        measurementlist = []
        measurementlist.append(measurement)
        self.laMea = dt.now()

        measurementlist = sorted(measurementlist, key=lambda x: x['t'])
        while (len(measurementlist) > 0):

            timest = measurementlist[0]['t']
            if (float(timest) > self.prevTime):
                if (self.testZero(measurementlist[0])):
                    self.prevTime = float(timest)
                    # print "The current time is " + str( time )
                    cn = measurementlist[0]['cn']
                    cv = measurementlist[0]['cv']
                    ce = measurementlist[0]['ce']
                    n = measurementlist[0]['n'] + self.Synth[0]
                    e = measurementlist[0]['e'] + self.Synth[1]
                    v = measurementlist[0]['v'] + self.Synth[2]
                    R = np.matrix([[cn, 0., 0.], [0., ce, 0.], [0., 0., cv]])
                    Mea = np.matrix([[n], [e], [v]])
                    res = Mea - self.H * self.Phi * self.State - self.H * self.Phi * self.State2
                    if ((np.abs(res[0, 0]) < self.MaxOffset) and (np.abs(res[1, 0]) < self.MaxOffset) and (
                        np.abs(res[2, 0]) < self.MaxOffset)):
                        if (self.First_mea == True):
                            self.FirstMea(Mea)
                            self.First_mea = False
                        else:
                            self.passMea(timest, Mea, R)
                    else:
                        print ("Site = " + str( self.NAME ) + " res = " + str( res[0,0] ) + " " + str( res[1,0] ) + " " + str( res[2,0 ] ))
            del measurementlist[0]
        return self.output

        '''try:
                url = self.urlst + 'records/' + self.streams[num] + self.urlen + str( self.lasttime )
                logging.info( self.NAME + " Testing url " + url )
                newurl = urllib2.urlopen( url, None, 60 )
                datadata = newurl.read()
                data = json.loads( datadata )
                if( len( data ) > 0 ):
                    update = True
                    try:
                        if( data['error'] ):
                            logging.error( self.NAME + " " + str( data['error'] ) )
                    except:
                        a = 0

                    try:
                        for recs in data['recs']:
                            t = float( recs['t'] )
                            m = np.matrix( [ [ 0. ], [ 0. ], [ 0. ] ] )
                            m[0,0] = recs['n']
                            m[1,0] = recs['e']
                            m[2,0] = recs['v']
                            r = np.matrix( [ [ 25., 0., 0.], [ 0., 25., 0.], [ 0., 0., 25. ] ] )
                            if( self.First == False ):
                                self.passMea( t, m, r )
                            else:
                                self.FirstMea( m )
                                self.First = False
                        self.lasttime = float( data['last'] )
                        logging.info( self.NAME + " finished processing" )
                    except:
                        logging.warning( self.NAME + " could not process data " )
            except:
                num = num + 1
                if( num == len( self.streams ) ):
                    logging.warning( self.NAME + " could not retrieve data" )
                    update = True'''

    def testZero(self, test):
        #logging.debug("Starting Kalman_testZero at time: {}".format(dt.now()))
        if (test['n'] != 0.):
            return True
        if (test['e'] != 0.):
            return True
        if (test['v'] != 0.):
            return True
        return False

    def Pause(self):
        return
        logging.debug("Starting Kalman_Pause at time: {}".format(dt.now()))
        self.ptime = dt.now()
        while (self.curtime - self.ptime < td(seconds=3)):
            time.sleep(2)
            # if( self.child_conn.poll() != False ):
            # 	Test = self.child_conn.recv()
            # 	if( Test == False ):
            # 		self.Running == False
            # 		break
            self.curtime = dt.now()
            # if( self.curtime - self.streamtime > td( minutes = 5 ) ):
            # 	self.UpdateStream()

    def UpdateStream(self):
        #logging.debug("Starting Kalman_UpdateStream at time: {}".format(dt.now()))
        streamlist = []
        strea = self.streams
        try:
            streamurl = self.urlst + "site/" + self.NAME
            streamopen = urllib2.urlopen(streamurl, None, 30)
            streamjson = streamopen.read()
            streamlist = json.loads(streamjson)
            self.streams = []
            for stream in streamlist['streams']:
                self.streams.append(stream.keys()[0])
            self.streamtime = self.curtime
            #logging.info(self.NAME + " updated streamlist")
        except:
            #logging.error(self.NAME + " could not update streamlist")
            self.streams = strea

    def KillFilter(self):
        #logging.debug("Starting Kalman_Killfilter at time: {}".format(dt.now()))
        print
        "Starting kill process " + str(self.NAME)
        self.KillMe = True
        return
        time.sleep(1)
        #self.ConPipe.send(['Kill', self.NAME])
        print
        "Request Sent for " + str(self.NAME)
        t = False
        while (t != True):
            t = self.ConPipe.recv()
            print
            "Kalman t = " + str(t)
            if (t == True):
                print
                str(self.NAME) + " " + str(self.RConn.poll())
                if (self.RConn.poll() == False):
                    logging.info("Kill Filter " + str(self.NAME))
                    print
                    "Kill Filter " + str(self.NAME)
                    self.FilterOff()
                    self.ConPipe.recv()
                    self.Running = False
                    self.run = False
                else:
                    logging.info("Don't Kill Filter " + str(self.NAME))
                    print
                    "Don't Kill Filter " + str(self.NAME)
                    self.ConPipe.send(False)
                    self.ConPipe.recv()
                    self.ConPipe.send(True)
                    self.KillMe = False
                    self.ConPipe.send("Resend")

    def FilterOff(self):
        #logging.debug("Starting Kalman_FilterOff at time: {}".format(dt.now()))
        Data = []
        Data.append(self.NAME)
        Data.append(self.K)
        Data.append(self.M)
        Data.append(self.P)
        Data.append(self.ResetP)
        Data.append(self.State)
        Data.append(self.State2)
        Data.append(self.IState)
        Data.append(self.IState2)
        Data.append(self.SMea)
        Data.append(self.offset)
        Data.append(self.Rcount)
        Data.append(self.InitP)
        Data.append(self.PCount)
        Data.append(self.SMCount)
        Data.append(self.EQCount)
        Data.append(self.prevTime)
        Data.append(self.Tag)
        Data.append(self.StartUp)
        self.ConPipe.send(Data)

    def UpdateData(self, Data):
        #logging.debug("Starting Kalman_UpdateData at time: {}".format(dt.now()))
        self.K = Data[1]
        self.M = Data[2]
        self.P = Data[3]
        self.ResetP = Data[4]
        self.State = Data[5]
        self.State2 = Data[6]
        self.IState = Data[7]
        self.IState2 = Data[8]
        self.SMea = Data[9]
        self.offset = Data[10]
        self.Rcount = Data[11]
        self.InitP = Data[12]
        self.PCount = Data[13]
        self.SMCount = Data[14]
        self.EQCount = Data[15]
        self.prevTime = Data[16]
        self.Tag = Data[17]
        self.StartUp = Data[18]

    def EQFlagTest(self):
        if (self.EQFlag[0, 0] == True):
            return True
        elif (self.EQFlag[1, 0] == True):
            return True
        elif (self.EQFlag[2, 0] == True):
            return True
        else:
            return False

    def EQNumTest(self):
        #logging.info("Starting Kalman_EQNumTest at time: {}".format(dt.now()))
        nu = self.EQCount[0, 0]
        if (self.EQCount[1, 0] > nu):
            nu = self.EQCount[1, 0]
        if (self.EQCount[2, 0] > nu):
            nu = self.EQCount[2, 0]
        return nu

    def FirstMea(self, Mea):
        #logging.debug("Starting Kalman_FirstMea at time: {}".format(dt.now()))
        self.State2 = Mea * 1.0
        # with open( './SiteData/' + self.NAME + '.d', 'w' ) as file:
        #	file.write('# Time NPos EPos VPos MeaN MeaE MeaV StdN StdE StdV KalSN KalSE KalSV PassN PassE PassV\n' )
        self.StartUp = True

    def passMea(self, Time, Mea, R):
        #logging.debug("Starting Kalman_passMea at time: {}".format(dt.now()))
        self.Ready = False
        self.Time = Time
        self.Mea = Mea
        self.R = R
        if (self.R[0, 0] < self.defR):
            self.R[0, 0] = self.defR
        if (self.R[1, 1] < self.defR):
            self.R[1, 1] = self.defR
        if (self.R[2, 2] < self.defR):
            self.R[2, 2] = self.defR
        if (self.offset == False):
            self.updateMat()
        else:
            self.passupdateState()

    def updateMat(self):
        self.output_state(print, "Old Kal BUM")
        #logging.info("Starting Kalman_updateMat at time: {}".format(dt.now()))
        if (self.prevTime != 0):
            self.delta_T = self.Time - self.prevTime
            self.prevTime = self.Time
            self.Q = np.matrix([[self.delta_T, 0., 0.], [0., self.delta_T, 0.], [0., 0., self.delta_T]])
        self.M = self.Phi * self.P * self.Phi.T + self.Q
        interm = (self.H * self.M * self.H.T + self.R).I
        self.K = self.M * self.H.T * interm
        self.P = (self.iden - self.K * self.H) * self.M
        if (self.DispWork == True):
            print
            'M = '
            print
            self.M
            print
            'Phi = '
            print
            self.Phi
            print
            'P = '
            print
            self.P
            print
            'K = '
            print
            self.K
            print
            'H = '
            print
            self.H
            print
            'R = '
            print
            self.R
            print
            'iden = '
            print
            self.iden
        self.output_state(print, "Old Kal AUM")
        self.calcRes()

    def calcRes(self):
        self.Res = self.Mea - self.H * self.Phi * self.State - self.H * self.Phi * self.State2
        if (self.DispWork == True):
            print
            'Mea = '
            print
            self.Mea
            print
            'Res = '
            print
            self.Res
        if (self.OverrideFlag == False):
            self.determineState()

    def determineState(self):
        self.output_state(print, "Old Kal DS")
        #logging.info("Starting Kalman_determineState at time: {}".format(dt.now()))
        # print self.SMCount
        # print self.smoothing
        if ((self.SMCount >= self.smoothing) and (self.StartUp == True)):
            self.StartUp = False
        if (self.SMCount < self.smoothing):
            self.EQCount = np.matrix([[0], [0], [0]])
            self.NormalMode()
            self.endProc()
        else:
            if (np.abs(self.Res[0, 0]) < np.sqrt(self.R[0, 0]) * self.EQThres):
                self.EQFlag[0, 0] = False
                self.EQCount[0, 0] = 0
            else:
                self.EQFlag[0, 0] = True
                self.EQCount[0, 0] = self.EQCount[0, 0] + 1
            if (np.abs(self.Res[1, 0]) < np.sqrt(self.R[1, 1]) * self.EQThres):
                self.EQFlag[1, 0] = False
                self.EQCount[1, 0] = 0
            else:
                self.EQFlag[1, 0] = True
                self.EQCount[1, 0] = self.EQCount[1, 0] + 1
            if (np.abs(self.Res[2, 0]) < np.sqrt(self.R[2, 2]) * self.EQThres):
                self.EQFlag[2, 0] = False
                self.EQCount[2, 0] = 0
            else:
                self.EQFlag[2, 0] = True
                self.EQCount[2, 0] = self.EQCount[2, 0] + 1
            if ((self.EQFlagTest() == True) and (self.EQNumTest() > self.Wait) and (self.offset == True)):
                # print 'EQNumTest = ' + str( self.EQNumTest() )
                # print 'Wait = ' + str( self.Wait )
                # print 'Comp = ' + str( self.EQNumTest() > self.Wait )
                self.EQState()
            elif ((self.EQFlagTest() == False) and (self.offset == True)):
                self.FalseEQState()
            elif ((self.EQFlagTest() == True) and (self.offset == False)):
                self.BeginEQTestState()
            else:
                self.NormalMode()
                self.endProc()

    def NormalMode(self):
        self.output_state(print, "Old Kal NM")
        #logging.info("Starting Kalman_NormalMode at time: {}".format(dt.now()))
        self.State = self.Phi * self.State + self.K * self.Res
        self.State2 = self.Phi * self.State2
        self.Tag = False
        if (self.DispWork == True):
            print
            'State = '
            print
            self.State
        '''if( self.OverrideFlag == True ):
            print self.Time
        elif( self.Time == 353094510.0 ):
            print str( self.Time ) + ' No Flag Override' '''
        if ((self.SMCount < self.smoothing) and (self.StartUp == False)):
            self.Tag = True
        self.send = {}
        self.send['site'] = self.NAME
        self.send['la'] = self.LAT
        self.send['lo'] = self.LON
        self.send['mn'] = self.Mea[0, 0]
        self.send['me'] = self.Mea[1, 0]
        self.send['mv'] = self.Mea[2, 0]
        self.send['kn'] = self.State[0, 0]
        self.send['ke'] = self.State[1, 0]
        self.send['kv'] = self.State[2, 0]
        self.send['cn'] = self.R[0, 0]
        self.send['ce'] = self.R[1, 1]
        self.send['cv'] = self.R[2, 2]
        self.send['he'] = self.HEI
        self.send['ta'] = self.Tag
        self.send['st'] = self.StartUp
        self.send['time'] = self.Time

    # self.StateData.append( [ self.Time, self.State[0,0] + self.State2[0,0], self.State[1,0] + self.State2[1,0], self.State[2,0] + self.State2[2,0], self.Mea[0,0], self.Mea[1,0], self.Mea[2,0], np.sqrt( self.P[0,0] ), np.sqrt( self.P[1,1] ), np.sqrt( self.P[2,2] ), self.State[0,0], self.State[1,0], self.State[2,0], self.State2[0,0], self.State2[1,0], self.State2[2,0], self.Tag ] )


    def EQState(self):
        self.output_state(print, "Old Kal PEQS")
        #logging.info("Starting Kalman_EQState at time: {}".format(dt.now()))
        if (self.EQPrint == True):
            print
            'Start EQ Process'
            print
            'Time = ' + str(self.Time)
            print
            'ResN = ' + str(self.Res[0, 0])
            print
            'ResE = ' + str(self.Res[1, 0])
            print
            'ResV = ' + str(self.Res[2, 0])
            print
            'StateN = ' + str(self.State[0, 0])
            print
            'StateE = ' + str(self.State[1, 0])
            print
            'StateV = ' + str(self.State[2, 0])
            print
            'RN = ' + str(np.sqrt(self.R[0, 0]))
            print
            'RE = ' + str(np.sqrt(self.R[1, 1]))
            print
            'RU = ' + str(np.sqrt(self.R[2, 2]))
        # with open( 'offsets.d', 'a' ) as file:
        #	file.write( '\t' + str( self.Time ) + ' ' + str( self.NAME ) + '\n' )
        # offsets.append( Tlist[num - WCount] )


        self.offsetreset()
        self.SMCount = 0
        # kalman.offset = True
        # Flag = False
        self.SMea.append([self.Time, self.Mea, self.R])
        # print 'SMea'
        # print self.SMea
        self.InitP = self.P[0, 0]
        self.P = self.ResetP * 1.0
        self.Pcount = 0.
        self.offset = False
        self.OverrideFlag = True
        while ((True) and (len(self.SMea) > 1)):
            self.R = self.SMea[0][2]
            self.Mea = self.SMea[0][1]
            self.Time = self.SMea[0][0]

            self.updateMat()
            self.NormalMode()

            # self.StateData.append( [ self.Time, self.State[0,0] + self.State2[0,0], self.State[1,0] + self.State2[1,0], self.State[2,0] + self.State2[2,0], self.Mea[0,0], self.Mea[1,0], self.Mea[2,0], np.sqrt( self.P[0,0] ), np.sqrt( self.P[1,1] ), np.sqrt( self.P[2,2] ), self.State[0,0], self.State[1,0], self.State[2,0], self.State2[0,0], self.State2[1,0], self.State2[2,0] ] )
            del self.SMea[0]
            # print 'SMea[0] = '
            # print self.SMea[0]
            # print len( self.SMea )
            if (len(self.SMea) == 1): break
        self.write = True
        self.R = self.SMea[0][2]
        self.Mea = self.SMea[0][1]
        self.Time = self.SMea[0][0]
        self.OverrideFlag = False
        self.SMea = []
        # self.updateMat( )
        self.output_state(print, "Old Kal AEQS")

    def FalseEQState(self):
        self.output_state(print, "Old Kal FEQS")
        if (self.EQPrint == True):
            print
            'Ending EQ test'
            print
            'ResN = ' + str(self.Res[0, 0])
            print
            'ResE = ' + str(self.Res[1, 0])
            print
            'ResV = ' + str(self.Res[2, 0])
            print
            'StateN = ' + str(self.State[0, 0])
            print
            'StateE = ' + str(self.State[1, 0])
            print
            'StateV = ' + str(self.State[2, 0])
            print
            'RN = ' + str(np.sqrt(self.R[0, 0]))
            print
            'RE = ' + str(np.sqrt(self.R[1, 1]))
            print
            'RU = ' + str(np.sqrt(self.R[2, 2]))

        self.write = True
        self.endpassState()
        self.OverrideFlag = True
        self.SMea.append([self.Time, self.Mea, self.R])
        while (len(self.SMea) > 1):
            self.R = self.SMea[0][2]
            self.Mea = self.SMea[0][1]
            self.Time = self.SMea[0][0]
            self.calcRes()
            self.NormalMode()

            # self.StateData.append( [ self.Time, self.State[0,0] + self.State2[0,0], self.State[1,0] + self.State2[1,0], self.State[1,0] + self.State2[2,0], self.Mea[0,0], self.Mea[1,0], self.Mea[2,0], np.sqrt( self.P[0,0] ), np.sqrt( self.P[1,1] ), np.sqrt( self.P[2,2] ), self.State[0,0], self.State[1,0], self.State[2,0], self.State2[0,0], self.State2[1,0], self.State2[2,0] ] )
            self.updateMat()
            del self.SMea[0]
            if (len(self.SMea) == 1): break
        self.offset = False
        self.R = self.SMea[0][2]
        self.Mea = self.SMea[0][1]
        self.Time = self.SMea[0][0]
        self.SMea = []
        self.OverrideFlag = False

    def BeginEQTestState(self):
        #logging.info("Starting Kalman_BeginEQTestSTate at time: {}".format(dt.now()))
        if (self.EQPrint == True):
            print
            'EQ potentially detected at time ' + str(self.Time)
            print
            'ResN = ' + str(self.Res[0, 0])
            print
            'ResE = ' + str(self.Res[1, 0])
            print
            'ResV = ' + str(self.Res[2, 0])
            print
            'StateN = ' + str(self.State[0, 0])
            print
            'StateE = ' + str(self.State[1, 0])
            print
            'StateV = ' + str(self.State[2, 0])
            print
            'RN = ' + str(np.sqrt(self.R[0, 0]))
            print
            'RE = ' + str(np.sqrt(self.R[1, 1]))
            print
            'RU = ' + str(np.sqrt(self.R[2, 2]))

        self.offset = True
        self.passStateStart()
        self.write = False

    def endProc(self):
        #logging.info("Starting Kalman_endProc at time: {}".format(dt.now()))
        if (self.offset == False):
            self.SMCount = self.SMCount + 1
            self.NormalMode()
        else:
            self.SMea.append([self.Time, self.Mea, self.R])
        # print 'EQNumTest = ' + str( self.EQNumTest() )
        # print self.SMea
        '''with open( './SiteData/' + self.NAME + '.d', 'a' ) as file:
            l = ''
            for num in range( len( self.StateData ) ):
                file.write( str( self.StateData[num] ) + '\n' )
                # l = l + str( self.StateData[num] ) + ' '
        '''
        # if( self.send ) in globals():
        # self.lock.acquire()
        # self.WConn.send( self.send )
        # self.lock.release()
        # print "Sending data " + str( self.NAME )
        self.output = self.send
        # logging.info( "Site " + str( self.NAME ) + " just sent data for timestep " + str( self.send['time'] ) )
        # print "Sent data for time " + str( self.send['time'] )
        del self.send

        # l = l[:-1] + '\n'
        # file.write( l )
        self.StateData = []
        self.Ready = True

    def passStateStart(self):
        #logging.info("Starting Kalman_passStateStart at time: {}".format(dt.now()))
        self.IState = self.State * 1.
        self.IState2 = self.State2 * 1.

    def passupdateState(self):
        #logging.info("Starting Kalman_passupdateState at time: {}".format(dt.now()))
        self.State = self.Phi * self.State
        self.State2 = self.Phi * self.State2
        self.calcRes()

    def endpassState(self):
        #logging.info("Starting Kalman_endpassState at time: {}".format(dt.now()))
        self.State = self.IState * 1.
        self.State2 = self.IState2 * 1.

    def offsetreset(self):
        #logging.info("Starting Kalman_offsetreset at time: {}".format(dt.now()))
        self.State2 = self.IState + self.IState2
        self.State = np.matrix([[0.], [0.], [0.]])

    # print 'OffsetReset'
    # print 'State = ' + str( self.State[0,0] )
    # print 'State2 = ' + str( self.State2[0,0] )

    def getReady(self):
        #logging.info("Starting Kalman_getReady at time: {}".format(dt.now()))
        # print self.Ready
        return self.Ready

    def output_state(self, output, message):
        return
        disp_names = ['h', 'phi', 'state', 'state_2', 'offset', 'max_offset', 'iden', 'k', 'm', 'p',
                     'measurement_matrix', 'r', 'def_r', 'synth_n', 'synth_e', 'synth_v',
                     'i_state', 'i_state_2', 'q', 'res', 'override_flag', 'sm_count',
                     'smoothing', 'start_up', 'eq_count', 'eq_flag', 'eq_threshold', 'tag',
                     's_measure', 'init_p', 'reset_p', 'p_count', 'time', 'write', 'wait']
        disp_list = ['self.H', 'self.Phi', 'self.State', 'self.State2', 'self.offset', 'self.MaxOffset', 'self.iden', 'self.K', 'self.M',
                     'self.P', 'self.Mea', 'self.R', 'self.defR', 'self.Synth[0]', 'self.Synth[1]', 'self.Synth[2]',
                     'self.IState', 'self.IState2', 'self.Q', 'self.Res', 'self.OverrideFlag', 'self.SMCount',
                     'self.smoothing', 'self.StartUp', 'self.EQCount', 'self.EQFlag', 'self.EQThres', 'self.Tag',
                     'self.SMea', 'self.InitP', 'self.ResetP', 'self.PCount', 'self.curtime',
                     'self.write', 'self.Wait']
        #output("Kalman state dump: {}".format(message))
        a = ""
        z = time.time()
        for idx, state in enumerate(disp_list):
            a = ("{} {}".format(disp_names[idx], eval(state)))
        print ("const {}".format(time.time() - z))
        z = time.time()
        print(a)
        print("appnd {}".format(time.time() - z))



def do_it():
    DataLoader.load_data_from_text_files()
    m = RabbitMQPoller()
    k = NewKalman()
    kalman_states = {}
    recent_data = []
    data_storage_size = 150
    empties = {}

    while True:
        data = m.get_next()
        recent_data.append(data)
        if data is not None:
            print("Time: {} ".format(data[0]['t']), end="")
            match = True
            nosendcount = 0
            for inv_run in DataStructures.inversion_runs:
                result_count_new = 0
                result_count_old = 0
                print (" {} ".format(inv_run['model']), end="")
                if inv_run['model'] in kalman_states:
                    inv_states = kalman_states[inv_run['model']]
                else:
                    kalman_states[inv_run['model']] = {}
                    inv_states = kalman_states[inv_run['model']]
                for d in data:
                    if d['site'] in inv_run['sites']:
                        if d['site'] in inv_states:
                            kal = inv_states[d['site']]['new']
                            o_kal = inv_states[d['site']]['old']
                        else:
                            kal = DataStructures.get_empty_kalman_state(inv_run)
                            o_kal = OldKalman()
                            o_kal.Wait = inv_run['mes_wait']
                            o_kal.smoothing = inv_run['eq_pause']
                            o_kal.EQThres = inv_run['eq_threshold']
                            o_kal.defR = inv_run['min_r']
                            o_kal.MaxOffset = inv_run['max_offset']
                            inv_states[d['site']] = {'new' : kal, 'old' : o_kal}
                        a = k.run(d, kal)
                        b = o_kal.getData(d)
                        if a != b:
                            match = False
                            print (d)
                            print("new {}".format(a))
                            print("old {}".format(b))
                        if a is not None:
                            result_count_new += 1
                            if a['time'] != data[0]['t']:
                                print (a['time'])
                            empties[d['site']] = 0
                        else:
                            if d['site'] in empties:
                                empties[d['site']] += 1
                                nosendcount = max(nosendcount, empties[d['site']])
                                #if empties[d['site']] > 10:
                                #    print (d)
                                #    for key, val in kal.items():
                                #        if key != 'sites' and key != 'run':
                                #            if key != 'data_set':
                                #                print ("{}:{}".format(key, val))
                                #            else:
                                #                print ("Data set: {}".format(len(val)))
                            else:
                                empties[d['site']] = 1
                        if b is not None:
                            result_count_old += 1
                print ("({}/{})".format(result_count_new, result_count_old), end="")

            if match:
                print ("All OK {}".format(nosendcount))
            else:
                print ("Fatal error, mismatch in Kalman filters.")
                filename = "{}-dump".format(data[0]['t'])
                print ("Dumping to file: {}".format(filename))
                with open(filename, 'w') as f:
                    json.dump(recent_data, f)
                return

        if len(recent_data) > data_storage_size:
            recent_data = recent_data[-data_storage_size:]
        else:
            time.sleep(.1)

do_it()
