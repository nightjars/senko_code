#! /usr/bin/python3
	
import subprocess as sub
import numpy as np
import math


def defpt( lat, lon, dep, strike, dip, rak, length, wid, slip, ten_slip, olat, olon, odep ):
	'''params should be a list in the form:
	latitude of middle of fault
	longitude(-180->180) of middle of the fault
	depth(>0, km) to middle of the fault
	strike(0-360)
	dip(0-90,90 is vertical;faultdips toward you if strike is to your right)
	rake(0->360) 0 is LL-SS, 90 is Reverse, 180 is RL-SS, 270 is Normal
	fault_length(in km)
	fault_width(in km)
	fault_slip(>0, meters)
	tensile_slip(m, >0 is opening)
	latitude of observation(deg,-180-180)
	longitude of observation(deg,-180-180)
	depth of observation(>0, km)
'''		
	#output has the form delta_N(meters)  delta_E(m)  delta_Z(m) dZ/dN(rad) dZ/dE(rad) Moment(dyne-cm) Mw Delta_sigma_s(bars)

	dlat = 110.574 * abs(lat - olat)
	dlon = 111.32 * math.cos(lat * np.pi/180)

	dis = ( dlat **2 + dlon **2 )**0.5

	if dis < 1000:

		#print("Dis = {}".format(dis))
		#print("./defpt.pl {} {} {} {} {} {} {} {} {} {} {} {} {}".format(lat, lon, dep, strike, dip, rak, length, wid, slip, ten_slip, olat, olon, odep ))
		out = sub.Popen("./defpt.pl {} {} {} {} {} {} {} {} {} {} {} {} {}".format(lat, lon, dep, strike, dip, rak, length, wid, slip, ten_slip, olat, olon, odep ), shell=True, stdout=sub.PIPE).stdout.read()
	#print("this is out: {}".format(out))
		out2 = out.split()

		dx = float(out2[0])
		dy = float(out2[1])
		dz = float(out2[2])

		mag = np.sqrt(dx**2 + dy**2 + dz**2)

	else:
		mag = 0
	#print("this is out2: {}".format(out2))
	return(mag)



def adp( subfault_lat, subfault_lon, subfault_depth, subfault_strike, subfault_dip,
		 subfault_rake, subfault_length, subfault_width, subfault_slip, site_lat, site_lon):
	if( subfault_lon > 180 ):
		subfault_lon -= 360
	if subfault_rake < 0:
		subfault_rake += 360
	if subfault_rake > 360:
		subfault_rake -= 360

	if( site_lon > 180 ):
		site_lon -= 360

	return defpt(subfault_lat, subfault_lon, subfault_depth, subfault_strike, subfault_dip,
				 subfault_rake, subfault_length, subfault_width, subfault_slip, 0,
				 site_lat, site_lon, 0)
