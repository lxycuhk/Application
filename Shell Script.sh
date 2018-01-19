# Shell Script to direct and run the Pig Latin programs
# The Pig Latin programs are used to calculate interested attributes of transportation card record 

#!/bin/sh

# Homepath are used to load Java Archive Files
homepath="/export/home/liuxinyi"

# Define parameters
path="/user/sunli/TOS/2017-04-01"
separator=","
field="card_id:chararray,deal_time:chararray,station_id:chararray,type:chararray,a:chararray"


save_day="2017-04-.*"

# The first i result to be filtered
i=10

# Size parameter, must be one of 10min、15min、30min、hour
size="15min"

# Define the time period for morning rush and evening rush
am_start="07:00:00"
am_end="09:00:00"
pm_start="17:00:00"
pm_end="20:00:00"

# Zone unit of the city, can choose from  locate440 440 transportation zones
#							          or  locate10  10 administrative district
Locate="locate10"

# Attributes to be calculated, can choose from: subway_flow_day,subway_flow_avg,subway_od_allday_avg_flow_top_i,subway_od_peek_avg_flow_top_i,subway_station_allday_in_out_flow,subway_station_mor_eve_in_flow_topi,subway_zone2zone_allday,subway_zone2zone_peek,subway_zone_in_out_allday,subway_zone_in_out_peek
#				subway_flow_day    passenger volume for each day                            output：is_holiday,date,flow
#				subway_size_flow   passenger volume for specified time period               output：sizetime,flow
#				subway_flow_avg    passenger volume per day average                         output：is_holiday,flow
#				subway_od_allday_avg_flow_top_i   find the OD whose passenger volume rankds top i   output：is_holiday,station_O,station_D,avg_flow
#				...

# Output path
output="/user/liuxinyi/SZoneday/test"

# Run Pig Latin program, define parameters and specify the output path
pig -p homepath=$homepath -p path=$path -p separator=$separator -p field=$field -p save_day=$save_day -p size=$size -p am_start=$am_start -p am_end=$am_end -p pm_start=$pm_start -p pm_end=$pm_end -p i=$i -p Locate=$Locate -p select_type=$select_type -p output=$output /state/partition1/home/liuxinyi/Cal_public_transit/subway/CalSubway_OD.pig