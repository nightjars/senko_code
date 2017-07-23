#!/usr/bin/perl -w
# sets up the parfile for defpt.c and then executes it 

@ARGV == 13 or die "usage: $0\n        latitude of middle of fault\n        longitude(-180->180) of middle of the fault\n        depth(>0, km) to middle of the fault\n        strike(0-360)\n        dip(0-90,90 is vertical;faultdips toward you if strike is to your right)\n        rake(0->360) 0 is LL-SS, 90 is Reverse, 180 is RL-SS, 270 is Normal\n        fault_length(in km)\n        fault_width(in km)\n        fault_slip(>0, meters)\n        tensile_slip(m, >0 is opening)\n        latitude of observation(deg,-180-180)\n        longitude of observation(deg,-180-180)\n        depth of observation(>0, km)\nOutput: delta_N(meters)  delta_E(m)  delta_Z(m) dZ/dN(rad) dZ/dE(rad) Moment(dyne-cm) Mw Delta_sigma_s(bars)\n";

$lat = shift;
$lon = shift;
$depth = shift;

$strike = shift;
$dip = shift;
$rake = shift;

$length = shift;
$width = shift;

$strike_slip_slip  = shift;
$tensile_slip = shift;

$lat_obs = shift;
$lon_obs = shift;
$depth_obs = shift;
$depth_obs = -$depth_obs;


if($lon>180){
	$lon = $lon-360;
}
                                                                                            

#convert to okada format
$al1=-$length/2;
$al2 = $length/2;
$aw1 = -$width/2;
$aw2 = $width/2;

$SS = sprintf("%.4f",$strike_slip_slip * cos(($rake)*.01744));
$DS = sprintf("%.4f",$strike_slip_slip * sin((180-$rake)*.01744));


open(INPUT_FH,"> ./defpt.input") or die "couldn't open input parfile for write\n";

print INPUT_FH "fault_cen_lat            $lat\n";
print INPUT_FH "fault_cen_lon            $lon\n";
print INPUT_FH "fault_cen_depth          $depth\n";
print INPUT_FH "strike                   $strike\n";
print INPUT_FH "dip                      $dip\n";
print INPUT_FH "ss_offset(m_pos_is_ll)   $SS\n";
print INPUT_FH "ds_(m_pos_is_thrust)     $DS \n";
print INPUT_FH "tensile_slip(meters)     $tensile_slip\n";
print INPUT_FH "left_edge(al1)           $al1\n";
print INPUT_FH "right_edge(al2)          $al2\n";
print INPUT_FH "downdip_edge(aw1)        $aw1\n";
print INPUT_FH "updip_edge(aw2)          $aw2\n";
print INPUT_FH "lat_obs                  $lat_obs\n";
print INPUT_FH "lon_obs                  $lon_obs\n";
print INPUT_FH "depth_obs                $depth_obs\n";
close(INPUT_FH);

system("./defpt.e ./defpt.input");
#system("./defpt.e ./defpt.input");
unlink("./defpt.input");

