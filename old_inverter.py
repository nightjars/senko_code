import subprocess as sub
from subprocess import PIPE, Popen
import numpy as np
import scipy as sp
from scipy import optimize
import math
from datetime import datetime as dt
import multiprocessing as mp
import time
import ok
from datetime import timedelta as td
import json
from multiprocessing import Lock
import threading as thr
import logging
import sys
import traceback
import threading


class TVLiveSlip:
    def __init__(self, IPipe, OPipe,
                 CPipe):  # starts things up. Defines variables, makes smoothing matrix if desired (put this in its own function)
        self.INDataPipe = IPipe
        self.OUTDataPipe = OPipe
        self.ConPipe = CPipe
        smoothing = True
        CornerFix = False
        shortSmoothing = True
        self.alpha = 1.0
        cutoff = 0.
        noise = 0.
        self.run = True
        self.maxChildren = 4.
        self.lock = Lock()
        self.inversionList = []
        self.invLock = Lock()
        self.inversionKillTime = 600.
        self.Convergence = -1.
        self.minOffset = -1.
        self.numFaults = -1.
        self.rangeThres = -1.
        self.SubfaultWid = 30.
        self.SubfaultLen = 60.
        self.label = ""
        self.model = ""
        self.tag = ""
        self.StrikeSlip = False

        self.Faults = []
        first = True
        with open('./subfaults.d', 'r') as file:
            while True:
                line = file.readline().split()
                if not line: break
                if ((first == True) and (line[0] != '#')):
                    self.SubfaultLen = float(line[0])
                    self.SubfaultWid = float(line[1])
                    first = False
                elif (line[0] != '#'):
                    self.Faults.append(line)

        sites = []
        with open('./sta_offset2.d', 'r') as file:
            while True:
                line = file.readline().split()
                if not line: break
                if (line[0] != '#'):
                    sites.append(line)

        sites.sort()

        self.numFaults = len(self.Faults)
        a = np.array([0.])
        Offset = a.copy()
        Offset.resize((1, 3))
        for con in range(3):
            Offset[0][con] = 0.

        logging.info("sites is this long: {}".format(len(sites)))
        logging.info("this is sites: {}".format(sites))
        logging.info("this is offset: {}".format(Offset))

        self.Correlate = []
        self.SubInputs = a.copy()
        self.SubInputs.resize(3, len(self.Faults))

        self.Correlate.append([sites[0][0], sites[0][1], sites[0][2]])
        curtime = dt.now()
        for con in range(len(self.Faults)):
            com = []
            com.append(float(self.Faults[con][0]))  # Lat 0
            com.append(float(self.Faults[con][1]))  # Lon 1
            com.append(float(self.Faults[con][2]))  # Dep 2
            com.append(float(self.Faults[con][3]))  # Str 3
            com.append(float(self.Faults[con][4]))  # Dip 4
            com.append(180)  # Rake 5
            com.append(float(self.Faults[con][5]))  # Len 6
            com.append(float(self.Faults[con][6]))  # Wid 7
            com.append(1)  # Slip 8
            com.append(0)  # Ten 9
            com.append(float(sites[0][1]))  # station Lat 10
            com.append(float(sites[0][2]))  # Station Lon 11
            com.append(0)  # station Depth 12

            com[5] = 0.
            info = ok.dc3d(com[0], com[1], com[2], com[3], com[4], com[5], com[6], com[7], com[8], com[9], com[10],
                           com[11], com[12])
            self.SubInputs[0][con] = float(info[0])
            self.SubInputs[1][con] = float(info[1])
            self.SubInputs[2][con] = float(info[2])

        Mask = a.copy()
        Mask.resize((3, 1))
        for num in range(3):
            Mask[num][0] = 0.

        self.smoothMat = a.copy()
        self.smoothMat.resize((len(self.Faults), len(self.Faults)))

        if (smoothing == True):
            self.smoothMat = a.copy()
            self.smoothMat.resize((len(self.Faults), len(self.Faults)))
            if (shortSmoothing == False):

                limit = math.sqrt(
                    math.pow(float(self.Faults[0][5]) / 111., 2) + math.pow(float(self.Faults[0][6]) / 111.,
                                                                            2) + math.pow(
                        float(self.Faults[0][2]) / 111., 2)) * 0.9
                for num in range(len(self.Faults)):
                    for con in range(len(self.Faults)):
                        self.smoothMat[num, con] = 0.
                for num in range(len(self.Faults)):
                    con = num + 1
                while (con < len(self.Faults)):
                    if (math.sqrt(math.pow(float(self.Faults[num][0]) - float(self.Faults[con][0]), 2) + math.pow(
                                float(self.Faults[num][1]) - float(self.Faults[con][1]), 2) + math.pow(
                                (float(self.Faults[num][2]) - float(self.Faults[con][2])) / 111., 2)) < limit):
                        self.smoothMat[num][con] = 1.
                        # self.smoothMat[num * 2 + 1,con * 2 + 1] = 1.
                        self.smoothMat[con][num] = 1.
                        # self.smoothMat[con * 2 + 1,num * 2 + 1] = 1.
                        self.smoothMat[num][num] = self.smoothMat[num, num] - 1.
                        # self.smoothMat[num * 2 + 1,num * 2 + 1] = self.smoothMat[num * 2 + 1,num * 2 + 1] - 1.
                        # self.smoothMat[con * 2 + 1,con * 2 + 1] = self.smoothMat[con * 2 + 1,con * 2 + 1] - 1.
                        self.smoothMat[con][con] = self.smoothMat[con, con] - 1.
                        con = con + 1
            else:
                for num in range(len(self.Faults)):
                    self.smoothMat[num][num] = 0
                    # self.smoothMat[num + 1, num + 1] = 0
                    if (num > self.SubfaultLen):
                        self.smoothMat[num][num] = -1
                        self.smoothMat[num - int(self.SubfaultLen)][num] = 1
                        self.smoothMat[num][num - int(self.SubfaultLen)] = 1
                    if (num < (self.SubfaultLen * (self.SubfaultWid - 1))):
                        self.smoothMat[num][num] = self.smoothMat[num][num] - 1
                        self.smoothMat[num + int(self.SubfaultLen)][num] = 1
                        self.smoothMat[num][num + int(self.SubfaultLen)] = 1
                    if (num % self.SubfaultLen != 0):
                        self.smoothMat[num][num] = self.smoothMat[num][num] - 1
                        self.smoothMat[num - 1][num] = 1
                        self.smoothMat[num][num - 1] = 1
                    if (num % self.SubfaultLen != self.SubfaultLen - 1):
                        self.smoothMat[num][num] = self.smoothMat[num][num] - 1
                        self.smoothMat[num + 1][num] = 1
                        self.smoothMat[num][num + 1] = 1

            if (CornerFix == True):
                for num in range(len(self.Faults)):
                    self.smoothMat[num][num] = -4

        self.AddMatrix = a.copy()
        self.AddMatrix.resize((int(self.SubfaultLen), len(self.Faults)))

        if (self.StrikeSlip == True):
            for num in range(len(self.SubfaultWid)):
                for con in range(len(self.SubfaultLen)):
                    self.AddMatrix[num + con * self.SubfaultWid][con] = 1.

        sit = 3
        fau = len(self.Faults)

        tempSubMat = a.copy()
        tempSubMat.resize((sit, fau))

        tempOffMat = a.copy()
        tempOffMat.resize((1, sit + fau))
        tempOffMat = []
        for num in range(3):
            tempOffMat.append(0.)

        tempMask = a.copy()
        tempMask.resize((sit + fau, 1))

        for num in range(sit):
            for con in range(fau):
                tempSubMat[num][con] = self.SubInputs[num][con]

        '''for num in range( fau ):
            for con in range( fau ):
                tempSubMat[sit + num][con] = self.smoothMat[num][con]'''

        for num in range(sit):
            tempOffMat[num] = Offset[0][num]

        tempMask.fill(0)

        for num in range(fau):
            tempMask[sit + num][0] = 1.

        Mask = tempMask.copy()

        self.InvSubInputs = tempSubMat.copy()

        self.Offset = tempOffMat

        self.stMask = Mask.copy()
        proclist = []

    # print "Self.SubInputs = " + str( self.SubInputs.shape )


    def add(self, station):
        t = ["Add", station]
        if (t[0] == "Add"):  # here's where the distance filtering takes place. This could go in its own function too.
            logging.info("TVSlip adding " + str(t[1]))
            line = ""
            a = np.array([0.])
            temp = a.copy()
            temp.resize((3, len(self.Faults)))

            count = 0

            with open('./sta_offset2.d', 'r') as file:
                while True:
                    line2 = file.readline().split()
                    if not line2: break
                    if (line2[0] == t[1]): break

            # mag = np.sqrt( info[0]**2 + info[1]**2 + info[2]**2 )
            # if( ( mag > self.minOffset ) ):
            # count += 1.

            with self.lock:
                self.SubInputs = np.vstack([self.SubInputs, temp])
                self.InvSubInputs = np.vstack([self.SubInputs, self.smoothMat])
                self.Correlate.append([line2[0], line2[1], line2[2]])

                self.Offset.append(0.)
                self.Offset.append(0.)
                self.Offset.append(0.)






    def Run(self):  # Over len( Files ) #This does the actual running. Basically, it launches CPipeWatcher, InversionWatcher and SingleInverter
        num = 0
        lock = Lock()

        #while (True):
            # print "Slip Inverter Checking for data"
        try:
            station = self.INDataPipe.recv()
            num += 1
            # print "Starting Inverter " + str( len( self.inversionList ) ) + " with children = " + str( len( mp.active_children() ) ) + " and maxChildren = " + str( self.maxChildren )
            while (len(
                    mp.active_children()) >= self.maxChildren):  # so as long as mp.active_children >= self.maxChildren, it won't do anything.
                time.sleep(0.1)
            p = threading.Thread(target=self.SingleInverter, args=(
            station, self.alpha, self.stMask, self.SubInputs, self.smoothMat, self.Offset, self.OUTDataPipe,
            self.Faults, self.Correlate, lock, self.AddMatrix))
            p.start()
            now = dt.now()
            self.inversionList.append([p, now, station[0][0]])
        except:
            cause = sys.exc_info()[1]
            for frame in traceback.extract_tb(sys.exc_info()[2]):
                fname, lineno, fn, text = frame
                logging.error("ERROR - {} {} {} {} {}".format(cause, fname, lineno, fn, text))

    def SingleInverter(self, station, alpha, stMask, SubInputs, smoothMat, Offset, Pipe, Faults, Correlate, lock,
                       AddMatrix):

        date = dt.now()
        logging.info("TVLiveSlip beginning inversion for " + str(station[0][0]))
        time = station[0][0]
        Mask = stMask.copy()
        npalpha = alpha
        npcutoff = 0.
        npnoise = 0.
        inv = 0
        a = np.array([0.])
        Mask = a.copy()
        k = np.shape(Offset)[0] + len(Faults)
        Mask.resize(k, 1)

        '''for i in range( len( Faults ) * 2 ):
            k = np.shape( Mask)[0] - i - 1
            Mask[ k, k ] = 1.'''

        for i in range(len(Faults)):
            k = np.shape(Mask)[0] - i - 1
            Mask[k][0] = 1.

        # print "Station = " + str( station )

        for n in station:  # this makes sure that the station matrix isn't empty and has the right lat/lon
            for m in Correlate:
                if n[1]['site'] == m[0]:
                    # print("Found one!")
                    n[1]['la'] = m[1]
                    n[1]['lo'] = m[2]
        lit = 0
        # print "Correlate 1 = " + str( len( Correlate ) )
        # print "Station = " + str( station )
        # print "Correlate = " + str( Correlate )
        con = 0
        while (con < len(Correlate)):
            start = lit
            while True:
                # print 'Station = ' + str( station[con][0] )
                # print 'Corr = ' + str( Correlate[lit][0]  )
                # print "Site = " + str( station[con][1]['site'] )
                # print "Correlate = " + str( Correlate[lit][0] )
                # print "Correlate len = " + str( len( Correlate ) )
                if (len(Correlate[con]) < 1):
                    print
                    "There is an error somewhere in Correlate"
                # print "Correlate = " + str( Correlate )
                # print "lit = " + str( lit )
                # print "Station len = " + str( len( station ) )
                # print "Con = " + str( con )
                # print "Station = " + str( station[con][1]['site'] )
                # print "Correlate = " + str( Correlate[lit][0] )
                # print str( station[con][1]['site'] == Correlate[lit][0] )
                if (station[lit][1]['site'] == Correlate[con][0]):
                    # print "Inside Loop"
                    # print Offset.shape
                    # print lit
                    # print len( Correlate )
                    Offset[con * 3] = float(station[lit][1]['kn'])
                    Offset[con * 3 + 1] = float(station[lit][1]['ke'])
                    Offset[con * 3 + 2] = float(station[lit][1]['kv'])
                    # print str( station[con][1]['ta'] )
                    if (station[lit][1]['ta'] == False):
                        Offset[con * 3] = 0.
                        Offset[con * 3 + 1] = 0.
                        Offset[con * 3 + 2] = 0.

                    # Mask[lit * 3][ 0 ] = 1.
                    # Mask[lit * 3 + 1][0] = 1.
                    # Mask[lit * 3 + 2][0 ] = 1.
                    # SubInputs = np.delete( SubInputs, [ lit * 3, lit * 3 + 1, lit * 3 + 2 ], 0 )
                    con += 1
                    inv += 1
                    break
                else:
                    # print "Start = " + str( start )
                    # print "lit = " + str( lit )
                    lit += 1
                    if (lit >= len(station)):
                        lit = 0
                    if (lit == start):
                        try:
                            start = lit
                            del Correlate[con]
                            SubInputs = np.delete(SubInputs, [con * 3, con * 3 + 1, con * 3 + 2], 0)
                            Offset = np.delete(Offset, [con * 3, con * 3 + 1, con * 3 + 2], 0)
                            if (con >= len(Correlate)):
                                break
                            '''if( lit >= len( st ) ):
                                lit = 0
                                start = lit'''
                        except:
                            cause = sys.exc_info()[1]
                            for frame in traceback.extract_tb(sys.exc_info()[2]):
                                fname, lineno, fn, text = frame
                                logging.error("ERROR - {} {} {} {} {}".format(cause, fname, lineno, fn, text))
                            logging.error(
                                "ERROR - Len Correlate = {} and Len SubInputs = {} and Len Offset = {} and lit = {} and con = {}".format(
                                    len(Correlate), len(SubInputs), len(Offset), lit, con))

                            # print lit
                            # time.sleep( 0.1 )
        # print SubInputs.shape
        SubInputs = np.vstack([SubInputs, smoothMat])

        # print "Got Here 2 in " + str( dt.now() - date )

        sttime = dt.now()

        # print "Solving Matrix " + str( sttime )
        # logging.info( "Mask dimensions = " + str( np.shape( Mask ) ) )
        # logging.info( "SubInputs dimensions = " + str( np.shape( SubInputs ) ) )
        # logging.info( "Offset dimensions = " + str( np.shape( Offset ) ) )
        # print "Mask = " + str( Mask )
        # print "SubInputs = " + str( SubInputs )
        # print "Offset = " + str( Offset )
        # Solution = a.copy()
        # Solution = np.linalg.lstsq( Mask * SubInputs, Mask * Offset )
        SI = SubInputs
        '''for num in Faults:
            Offset = np.append( Offset, 0. )'''
        OF = Offset
        for num in Faults:
            OF = np.append(OF, 0.)
        # OF.flatten()
        # OF.transpose()
        # print type( Offset )
        # print Offset.shape
        # print Offset
        # OF = np.ndarray( OF )
        '''for con in xrange( Mask.shape[0] ):
            if( Mask[con][0] == 0 ):
                SI = np.delete( SI, [ con * 3, con * 3 + 1, con * 3 + 2 ], 0 )
                OF = np.delete( OF, [ con * 3, con * 3 + 1, con * 3 + 2 ], 0 )'''

        invbegin = dt.now()
        Solution = sp.optimize.nnls(SI, OF)  # the big moment. This does the matrix math necessary.
        invend = dt.now()
        # print Solution
        Solution = Solution[0]
        # print Solution.shape
        print
        "Inversion finished in " + str(invend - invbegin)
        # Solution = np.matrix( Solution[0] )
        # Solution = Solution.transpose()
        # print Solution
        # print Solution.shape
        # print SubInputs.shape
        # logging.info( Solution )
        # print "Solution = " + str( Solution )
        # print SubInputs
        FaultSol = []
        curtime = dt.now()
        ttime = curtime - sttime
        # print "Matrix Solved " + str( curtime ) + " in " + str( ttime )

        # Reverse = []
        # Reverse = SubInputs * Solution[0]


        CalcOffset = SubInputs.dot(Solution)
        # print SubInputs.shape
        # print Solution.shape
        # print CalcOffset.shape

        # logging.info("This is solution: {}".format(solution))


        for con in range(len(Solution)):
            FaultSol.append([])
            FaultSol[con].append(Faults[con][0])
            FaultSol[con].append(Faults[con][1])
            FaultSol[con].append(Faults[con][2])
            FaultSol[con].append(Faults[con][3])
            FaultSol[con].append(Faults[con][4])
            '''rake = 0.
            if( Solution[0][con * 2,0] != 0 ):
                rake = np.arctan( ( Solution[0][con * 2 + 1,0] / Solution[0][con * 2,0] ) ) / np.pi * 180.
                # print rake
            rake = float( rake )
            if( rake < 0. ):
                rake = rake + 180.
            elif( Solution[0][con * 2 + 1,0] > 0 ):
                rake = 90.
            elif( Solution[0][con * 2 + 1,0] < 0 ):
                rake = 270.
            if( ( Solution[0][con * 2,0] > 0 ) and ( Solution[0][con * 2 + 1,0] == 0 ) ):
                rake = 0.
            elif( ( Solution[0][con * 2,0] < 0 ) and ( Solution[0][con * 2 + 1,0] == 0 ) ):
                rake = 180.

            if( Solution[0][con * 2 + 1,0] < 0 ):
                rake = rake + 180.
                # print rake'''

            rake = self.Faults[con][7]

            FaultSol[con].append(str(rake))
            FaultSol[con].append(Faults[con][5])
            FaultSol[con].append(Faults[con][6])
            # slip = np.sqrt( np.square( Solution[0][con ,0] ) + np.square( Solution[0][con + 1,0] ) )
            Zero = False
            '''if( rake > 180. ):
                slip = slip * -1.
            if( slip < 0.001 ):
                slip = 0.
                Zero = True'''
            slip = Solution[con]
            FaultSol[con].append(str(slip))
            FaultSol[con].append("0")
            if (Zero == False):
                # print "Taking Actual Data"
                FaultSol[con].append(Solution[con])
            # FaultSol[con].append( Solution[con  + 1,0] )
            else:
                # print "Zeroing"
                FaultSol[con].append(0.)
                # FaultSol[con].append( 0. )

        # print FaultSol

        '''with open( './SlipData/' + File, 'w' ) as file:
            l = '# Lat Lon dep str Dip Rake Len Wid Slip Ten SSlip DSlip\n'
            file.write( l )
            l = '# alpha ' + str( npalpha ) + '\n'
            file.write( l )
            l = '# cutoff ' + str( npcutoff ) + '\n'
            file.write( l )
            l = '# noise ' + str( npnoise ) + '\n'
            file.write( l )
            for con in range( len( FaultSol )):
                l = ''
                for lit in range( len( FaultSol[con] ) ):
                    l = l + str( FaultSol[con][lit] ) + ' '
                    file.write( l[:-1] + '\n' )'''

        FinalCalc = []
        num = 0

        for con in range(len(station)):
            lit = 0
            while True:
                # print "Length of stations = " + str( len( station[con] ) )
                if (station[con][1]['site'] == Correlate[lit][0]):
                    # print CalcOffset[lit * 3][0]
                    # print "CalcOffset = " + str( len( CalcOffset ) )
                    FinalCalc.append(
                        [station[con][1]['site'], station[con][1]['la'], station[con][1]['lo'], station[con][1]['he'],
                         station[con][1]['kn'], station[con][1]['ke'], station[con][1]['kv'], CalcOffset[lit * 3],
                         CalcOffset[lit * 3 + 1], CalcOffset[lit * 3 + 2]])
                    if (station[con][1]['ta'] == False):
                        # print "Zeroing"
                        # print len( FinalCalc[num] )
                        FinalCalc[num][4] = 0.
                        FinalCalc[num][5] = 0.
                        FinalCalc[num][6] = 0.
                    # print "Final Calc = " + str( FinalCalc[con] )
                    num += 1
                    break
                else:
                    lit += 1
                if (lit == len(Correlate)):
                    break
        # print len( FinalCalc )
        # print FinalCalc
        # print FaultSol

        '''with open( './CalcOffset/' + File, 'w' ) as file:
            l = '# Site Lat Lon Hei N E V Calc( N E V )\n'
            file.write( l )
            l = '# alpha ' + str( npalpha ) + '\n'
            file.write( l )
            l = "# cutoff " + str( npcutoff ) + '\n'
            file.write( l )
            l = "# noise " + str( npnoise ) + '\n'
            file.write( l )
            for con in range( len( FinalCalc ) ):
                l = ''
                for lit in range( len( FinalCalc[con] )):
                    l = l + str( FinalCalc[con][lit] ) + ' '
            file.write( l[:-1] + '\n' )'''

        send = {}

        short = []

        for lit in station:
            short.append(
                [lit[1]['site'], lit[1]['kn'], lit[1]['ke'], lit[1]['kv'], lit[1]['ta'], lit[1]['mn'], lit[1]['me'],
                 lit[1]['mv'], lit[1]['cn'], lit[1]['ce'], lit[1]['cv']])
        send['data'] = short
        if (station[0][1]['time']):
            # print station[0][1]['time']
            send['time'] = station[0][1]['time']

        short = []

        fin = dt.utcfromtimestamp(float(send['time']))

        don = fin.strftime("%Y-%m-%d %H:%M:%S %Z")

        send['label'] = self.label + " " + self.model + ' - ' + don + "UTC"

        Magnitude = 0.0

        for con in FaultSol:
            Magnitude = Magnitude + float(con[6]) * float(con[7]) * np.abs(float(con[8])) * float(1e12)

        Magnitude = Magnitude * float(3e11)

        if (Magnitude != 0):
            print("Magnitude = {}".format(Magnitude))

        if (self.StrikeSlip == False):
            for lit in FaultSol:
                # short.append( lit[8] )
                short.append(lit)
        else:
            for lit in range(int(self.SubfaultLen)):
                temp = FaultSol[lit]
                temp[8] = float(temp[8])
                for num in range(int(self.SubfaultWid)):
                    temp[8] = temp[8] + float(FaultSol[lit + num * int(self.SubfaultLen)][8])
                short.append(temp)
        send['slip'] = short
        short = []
        for lit in FinalCalc:
            # short.append( [ lit[0], lit[7], lit[8] ] )
            short.append([lit[0], lit[1], lit[2], lit[3], lit[4], lit[5], lit[6], lit[7], lit[8], lit[9]])
        send['estimates'] = short

        # logging.debug("send['estimates'] is this: {}".format(send['estimates']))

        # send['extras'] = station

        '''fin = json.dumps( send )

        fin = {}
        if( station[0][1]['time'] ):
            print station[0][1]['time']
            fin['t'] = station[0][1]['time']
            fin['result'] = jsonres'''

        # print "Stations Len = " + str( len( station ) ) + " and Computed Len = " + str( len( FinalCalc ) ) + " and Inverted Number = " + str( inv )

        Mw = 0.
        if (Magnitude != 0.):
            Mw = 2. / 3. * np.log10(Magnitude) - 10.7

            # Moment = ( Magnitude / cat ) // 1 *  cat
            # Mw = ( Mw * 10 ) // 1 / 10
            Magnitude = "{:.2E}".format(Magnitude)
            Mw = "{:.1f}".format(Mw)
        # Magnitude = str( Moment )
        else:
            Mw = "NA"
            Magnitude = "{:.2E}".format(Magnitude)

        # print "Mw = " + str( Mw )
        # print "Magnitude = " + str( Moment )

        '''if( Mw > 5. ):
        p = sub.Popen( [ 'mail', '-s', 'Inverter', 'norfordb@cwu.edu' ], stdin = PIPE )
        p.communicate( 'The Inverter has detected a slip > Mw 6.0 at time ' + str( datetime.now() ) )
        p.wait()'''

        send['Moment'] = Magnitude

        send['Magnitude'] = Mw
        self.OUTDataPipe.send(send)
