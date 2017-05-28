import ok1
import numpy as np


def dc3d( lat, lon, depth, strike, dip, rake, fault_length, fault_width, fault_slip, tensile_slip, obs_lat, obs_lon, obs_dep ):
	MU = float( 3e11 )

	strike = np.pi * strike / 180.
	rake_rad = np.pi * rake / 180.
	lat_rad = np.pi * lat / 180.
	obs_lat_rad = np.pi * obs_lat / 180.



	alpha = 0.66666
	if( lon > 180. ):
		lon = lon - 360.
	lon_rad = np.pi * lon / 180.

	if( obs_lon > 180. ):
		obs_lon = obs_lon - 360.
	obs_lon_rad = np.pi * obs_lon / 180.

	northing = 111 * ( obs_lat - lat )
	easting = 111 * np.cos( lat_rad ) * ( obs_lon - lon )
	length = fault_length / 2.
	width = fault_width / 2.

	px = northing * np.cos( strike ) + easting * np.sin( strike )
	py = northing * np.sin( strike ) - easting * np.cos( strike )

	SS = fault_slip * np.cos( rake_rad )
	DS = fault_slip * np.sin( np.pi - rake_rad )

	obs_dep = -obs_dep


#dc3d(alpha, x, y, z, depth, dip, al1, al2, aw1, aw2, disl1, disl2, disl3,
    # ux, uy, uz,uxx, uyx, uzx, uxy, uyy, uzy, uxz, uyz, uzz, iret)


	result = ok1.dc3d( alpha, px, py, obs_dep, depth, dip, -1 * length,
                       length, -1 * width, width, SS, DS, tensile_slip)

	ux = result[0]
	uy = result[1]
	uz = result[2]
	uxx = result[3]
	uyx = result[4]
	uzx = result[5]
	uxy = result[6]
	uyy = result[7]
	uzy = result[8]
	uxz = result[9]
	uyz = result[10]
	uzz = result[11]


	east = ux * np.sin( strike ) - uy * np.cos( strike )
	north = uy * np.sin( strike ) + ux * np.cos( strike )
	vertical = uz

	north = north // float( 1e-5 ) / float( 1e5 )
	east = east // float( 1e-5 ) / float( 1e5 )
	vertical = vertical // float( 1e-5 ) / float( 1e5 )

	if( np.abs( east ) < float( 1e-4 ) ):
		east = 0.
	if( np.abs( north ) < float( 1e-4 ) ):
		north = 0.
	if( np.abs( vertical ) < float( 1e-4 ) ):
		vertical = 0.

	dzdn = uzx * np.cos( strike ) + uzy * np.sin( strike )
	dzde = uzx * np.sin( strike ) - uzy * np.cos( strike )

	dzdn = np.arctan( dzdn / 1000 )
	dzde = np.arctan( dzde / 1000 )



	temp1 = np.sqrt( np.power( SS, 2 ) + np.power( DS, 2 ) + np.power( tensile_slip, 2 ) )
	temp2 = fault_width * fault_length * float( 1e10 )

	moment = MU * temp1 * 100 * temp2
	moment_mag = np.log10( moment ) / 1.5 - 10.73

	stress_drop = ( 2 * MU * temp1 * 100 / np.sqrt( temp2 ) / float( 1e6  ))
	res = [ north, east, vertical, dzdn, dzde, moment, moment_mag, stress_drop ]

	return res