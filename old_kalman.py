import numpy as np
from numpy import matlib
#import urllib2
import json
from datetime import datetime as dt
from datetime import timedelta as td
import logging
import time
from multiprocessing import Lock, Queue
import threading as thr
import logging
import sys
import traceback


# import scipy as sp
# from scipy import linalg

class Kalman:
    def __init__(self):
        logging.info("initiating a Kalman process")
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

    def setName(self, n):
        self.NAME = str(n)

    # print "Site = " + str( self.NAME )

    def Init_Filter(self, router_conn, writer_conn, controller_conn):
        logging.debug("Starting Kalman_Init_Filter at time: {}".format(dt.now()))
        self.RConn = router_conn
        self.WConn = writer_conn
        self.ConPipe = controller_conn
        self.laMea = dt.now()
        setup = False
        '''while( setup == False ):
            try:
                url = self.urlst + "site/" + str( self.NAME )
                print url
                newurl = urllib2.urlopen( url, None, 60 )
                print "Got new url"
                datadata = newurl.read()
                print "Read url"
                data = json.loads( datadata )
                print "Loaded json"
                self.LAT = data['coor']['la']
                print "Got Lat"
                self.LON = data['coor']['lo']
                print "Got Lon"
                setup = True
                print "Got Data"
            except:
                pass'''
        with open('./sta_offset2.d', 'r') as file:
            while True:
                line = file.readline().split()
                if not line: break
                if (line[0] == self.NAME):
                    self.LAT = float(line[2])
                    self.LON = float(line[1])
                    # print "LAT/LON set for " + str( self.NAME )
                    break
        if (self.LAT == 0):
            self.LAT = -120
        if (self.LON == 0):
            self.LON = 48

    def InitFilter(self, sttime):
        self.lasttime = sttime
        self.Live = True
        self.First = True

    def __CPipeWatcher(self):
        logging.debug("Starting Kalman__CPipeWatcher at time: {}".format(dt.now()))
        while (self.run == True):
            if (self.KillMe == False):
                if (self.ConPipe.poll() == True):
                    t = self.ConPipe.recv()
                    if (t == True):
                        self.ConPipe.send(False)
                    elif (t != None):
                        if (t[0] == "EQPause"):
                            self.smoothing = float(t[1])
                        if (t[0] == "EQThres"):
                            self.EQThres = float(t[1])
                        if (t[0] == "MesWait"):
                            self.Wait = float(t[1]) + 1.
                        if (t[0] == "DieTime"):
                            self.DieTime = float(t[1])
                        if (t[0] == "MinR"):
                            self.defR = float(t[1])
                        if (t[0] == "Offset"):
                            if (t[1] == "True"):
                                with open('./Offsets.d', 'r') as file:
                                    end = False
                                    while (end == False):
                                        line = file.readline().split()
                                        if not line:
                                            logging.warning("Could not find Synthetic offset for site " + str(
                                                self.NAME) + " in offset file")
                                            break
                                        if (str(self.NAME) == line[0]):
                                            self.Synth[0] = float(line[1])
                                            self.Synth[1] = float(line[2])
                                            self.Synth[2] = float(line[3])
                                            end = True
                                            logging.info(
                                                "Found Synthetics for " + str(self.NAME) + " of " + str(self.Synth))
                            elif (t[1] == "False"):
                                self.Synth[0] = 0.
                                self.Synth[1] = 0.
                                self.Synth[2] = 0.
                        if (t[0] == "MaxOffset"):
                            self.MaxOffset = float(t[1])
                else:
                    time.sleep(1)
            else:
                time.sleep(1)
        logging.warning(self.NAME + " CPipe Ending")
        print
        "Ending CPipeWatcher for " + str(self.NAME)

    # def FilterOn( self, conn, lo ):
    def FilterOn(self):
        logging.debug("Starting Kalman_FilterOn at time: {}".format(dt.now()))
        conn = self.WConn
        lo = 0.
        t = thr.Thread(target=self.__CPipeWatcher)
        t.start()
        self.lock = lo
        self.child_conn = conn
        self.Running = True
        self.ptime = dt.now()
        self.curtime = dt.now()
        # self.UpdateStream()
        while (self.Running == True):
            self.getData()
            self.Pause()
            now = dt.now()
            #if ((now - self.laMea) > td(seconds=self.DieTime)):
            #   self.KillFilter()
        t.join()
        logging.info(self.NAME + " Filter Ending")
        print
        "Exiting Filter " + str(self.NAME)

    def getData(self):
        logging.debug("Starting Kalman_getData at time: {}".format(dt.now()))
        update = False
        num = 0
        measurementlist = []

        # while( self.RConn.poll() == True ):
        try:
            while (self.RConn.poll() == True):
                # print "Redieving data " + str( self.NAME )
                l = self.RConn.recv()
                measurementlist.append(l)
                logging.info("received {}".format(l))
                # print "Measurements Recieved"
                self.laMea = dt.now()
        except:
            logging.info(self.NAME + " recieved data that could not be processed.")
            cause = sys.exc_info()[1]
            for frame in traceback.extract_tb(sys.exc_info()[2]):
                fname, lineno, fn, text = frame
                logging.error("ERROR - {} {} {} {} {}".format(cause, fname, lineno, fn, text))

        measurementlist = sorted(measurementlist, key=lambda x: x['t'])
        while (len(measurementlist) > 0):
            try:
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
                                # else:
                                # print "Site = " + str( self.NAME ) + " res = " + str( res[0,0] ) + " " + str( res[1,0] ) + " " + str( res[2,0 ] )
                del measurementlist[0]
            except:
                logging.error(self.NAME + " could not process a measurement.")
                print
                "SMea = " + str(self.SMea)
                cause = sys.exc_info()[1]
                for frame in traceback.extract_tb(sys.exc_info()[2]):
                    fname, lineno, fn, text = frame
                    logging.error("ERROR - {} {} {} {} {}".format(cause, fname, lineno, fn, text))
                del measurementlist[0]

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
        logging.debug("Starting Kalman_testZero at time: {}".format(dt.now()))
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
        logging.debug("Starting Kalman_UpdateStream at time: {}".format(dt.now()))
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
            logging.info(self.NAME + " updated streamlist")
        except:
            logging.error(self.NAME + " could not update streamlist")
            self.streams = strea

    def KillFilter(self):
        logging.debug("Starting Kalman_Killfilter at time: {}".format(dt.now()))
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
        logging.debug("Starting Kalman_FilterOff at time: {}".format(dt.now()))
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
        logging.debug("Starting Kalman_UpdateData at time: {}".format(dt.now()))
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
        logging.info("Starting Kalman_EQNumTest at time: {}".format(dt.now()))
        nu = self.EQCount[0, 0]
        if (self.EQCount[1, 0] > nu):
            nu = self.EQCount[1, 0]
        if (self.EQCount[2, 0] > nu):
            nu = self.EQCount[2, 0]
        return nu

    def FirstMea(self, Mea):
        logging.debug("Starting Kalman_FirstMea at time: {}".format(dt.now()))
        self.State2 = Mea * 1.0
        # with open( './SiteData/' + self.NAME + '.d', 'w' ) as file:
        #	file.write('# Time NPos EPos VPos MeaN MeaE MeaV StdN StdE StdV KalSN KalSE KalSV PassN PassE PassV\n' )
        self.StartUp = True

    def passMea(self, Time, Mea, R):
        logging.debug("Starting Kalman_passMea at time: {}".format(dt.now()))
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
        logging.info("Starting Kalman_updateMat at time: {}".format(dt.now()))
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
        logging.info("Starting Kalman_determineState at time: {}".format(dt.now()))
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
        logging.info("Starting Kalman_NormalMode at time: {}".format(dt.now()))
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
        logging.info("Starting Kalman_EQState at time: {}".format(dt.now()))
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

    def FalseEQState(self):
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
        logging.info("Starting Kalman_BeginEQTestSTate at time: {}".format(dt.now()))
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
        logging.info("Starting Kalman_endProc at time: {}".format(dt.now()))
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
        try:
            logging.info("Sending: {}".format(self.send))
            self.WConn.send(self.send)
        # logging.info( "Site " + str( self.NAME ) + " just sent data for timestep " + str( self.send['time'] ) )
        except:
            logging.error("Site " + str(self.NAME) + " could not send data for time " + str(self.send['time']))
        # print "Sent data for time " + str( self.send['time'] )
        del self.send

        # l = l[:-1] + '\n'
        # file.write( l )
        self.StateData = []
        self.Ready = True

    def passStateStart(self):
        logging.info("Starting Kalman_passStateStart at time: {}".format(dt.now()))
        self.IState = self.State * 1.
        self.IState2 = self.State2 * 1.

    def passupdateState(self):
        logging.info("Starting Kalman_passupdateState at time: {}".format(dt.now()))
        self.State = self.Phi * self.State
        self.State2 = self.Phi * self.State2
        self.calcRes()

    def endpassState(self):
        logging.info("Starting Kalman_endpassState at time: {}".format(dt.now()))
        self.State = self.IState * 1.
        self.State2 = self.IState2 * 1.

    def offsetreset(self):
        logging.info("Starting Kalman_offsetreset at time: {}".format(dt.now()))
        self.State2 = self.IState + self.IState2
        self.State = np.matrix([[0.], [0.], [0.]])

    # print 'OffsetReset'
    # print 'State = ' + str( self.State[0,0] )
    # print 'State2 = ' + str( self.State2[0,0] )

    def getReady(self):
        logging.info("Starting Kalman_getReady at time: {}".format(dt.now()))
        # print self.Ready
        return self.Ready

    def output_state(self, output, message):
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
        output("Kalman state dump: {}".format(message))
        for idx, state in enumerate(disp_list):
            output("{} {}".format(disp_names[idx], eval(state)))
