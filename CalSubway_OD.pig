-- The Pig Lating program is used to calculate several attributes of transportation card record of subway in Shenzhen


-- Java Archive Files that may be used
REGISTER $homepath/Cal_public_transit/jars/datafu-1.2.0.jar;
REGISTER $homepath/Cal_public_transit/jars/piggybank.jar;
REGISTER $homepath/Cal_public_transit/jars/joda-time-1.6.jar;
REGISTER $homepath/Cal_public_transit/jars/TimeChange.jar;
REGISTER $homepath/Cal_public_transit/jars/isFestivals.jar;
REGISTER $homepath/Cal_public_transit/jars/Locate440.jar;
REGISTER $homepath/Cal_public_transit/jars/Locate2.jar;
REGISTER $homepath/Cal_public_transit/jars/TimeCompare.jar;
DEFINE timeME com.wangjie.myUDF.timeME();
DEFINE timeL com.wangjie.myUDF.timeL();

DEFINE ISFestivals huzhen.ISFestivals();
DEFINE changehour zhangjun.TimeChange.changehour();
DEFINE MarkovPairs datafu.pig.stats.MarkovPairs();
DEFINE ISOSecondsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween();
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE locate440 sunli.jar.Locate440();
DEFINE locate10 huzhen.locate.Locate();
DEFINE change$size zhangjun.TimeChange.change${size}();

--parameters input,save_day,size,i,Locate,select_type,output


--original data is like:/datum/szt/subway/20170320:8059170,666010567,267032107,21,2017-03-17T20:05:46.000Z,地铁九号线,文锦,None
--load data
tos_data_rec = LOAD '$path' USING PigStorage('$separator') AS ($field);

--abstract necessary field
tos_data_for = FOREACH tos_data_rec GENERATE card_id,deal_time,station_id,type;



--------everyday's passenger volume-------------
subway_flow_grp = GROUP subway_final BY (is_holiday,date);
subway_flow_day_for = FOREACH subway_flow_grp GENERATE  FLATTEN(group) AS (is_holiday,date),(long)(COUNT(subway_final)*1.0/0.87*1.52) AS flow:long;
subway_flow_day = ORDER subway_flow_day_for BY date;


-------passenger volume of specified time period------------
subway_size = FOREACH subway_final GENERATE change$size(o_time) AS changetime:chararray;
subway_size_grp = GROUP subway_size BY changetime;
subway_size_flow_for = FOREACH subway_size_grp GENERATE group AS changetime,(long)(COUNT(subway_size)*1.0/0.87*1.52) AS flow:long;
subway_size_flow = ORDER subway_size_flow_for BY changetime;


-- passenger volume for each origin-destination (OD) per month
  -- weekday + weekend + holiday
subway_yuejun_grp = GROUP subway_everyday_gen1 BY (is_holiday,station_O,station_D);
subway_yuejun_gen = FOREACH subway_yuejun_grp GENERATE FLATTEN(group) AS (is_holiday:chararray,station_O:chararray,station_D:chararray),(long)AVG(subway_everyday_gen1.flow) AS avg_num:long;

    --top10
subway_yuejun_ord_grp = GROUP subway_yuejun_gen BY (is_holiday);
subway_od_allday_avg_flow_top_i = FOREACH subway_yuejun_ord_grp {
	sort = ORDER subway_yuejun_gen BY avg_num DESC ;
	top10 = LIMIT sort $i;
	GENERATE FLATTEN(top10) AS (is_holiday:chararray,station_O:chararray,station_D:chararray,avg_num:long);
};

--peek--
subway_everyday_grp2 = GROUP subway_final BY (date,changetime,is_holiday,station_name1,station_name2);
subway_everyday_gen1 = FOREACH subway_everyday_grp2 GENERATE FLATTEN(group) AS (date:chararray,changetime:chararray,is_holiday:chararray,station_O:chararray,station_D:chararray),(long)(COUNT(subway_final)*1.0/0.87) AS flow:long;

subway_yuejun_peek_grp = GROUP subway_everyday_gen1 BY (changetime,is_holiday,station_O,station_D);
subway_yuejun_peek_gen = FOREACH subway_yuejun_peek_grp GENERATE FLATTEN(group) AS (changetime:chararray,is_holiday:chararray,station_O:chararray,station_D:chararray),(long)AVG(subway_everyday_gen1.flow) AS avg_num:long;

    --top10
subway_yuejun_peek_ord_grp = GROUP subway_yuejun_gen_lit BY is_holiday;
subway_od_peek_avg_flow_top_i = FOREACH subway_yuejun_peek_ord_grp {
	sort = ORDER subway_yuejun_gen_lit BY avg_num DESC ;
	top10 = LIMIT sort $i;
	GENERATE FLATTEN(top10) AS (changetime:chararray,is_holiday:chararray,station_O:chararray,station_D:chararray,avg_num:long);
};



--passenger volume that enter/exit station for all day
-- enter station, average
subway_shang1_grp = GROUP subway_final BY (date,is_holiday,station_name1);
subway_shang1_gen = FOREACH subway_shang1_grp GENERATE FLATTEN(group) AS (date:chararray,is_holiday:chararray,station_O:chararray),(long)(COUNT(subway_final)*1.0/0.87) AS flow:long;
 
  -- volume per month
    -- weekday + weekend + holiday
subway_shang_grp = GROUP subway_shang1_gen BY (is_holiday,station_O);
subway_shang_gen = FOREACH subway_shang_grp GENERATE FLATTEN(group) AS (is_holiday:chararray,station_O:chararray),(long)AVG(subway_shang1_gen.flow) AS avg_num:long;


-- exit station, average
subway_xia1_grp = GROUP subway_final BY (date,is_holiday,station_name2);
subway_xia1_gen = FOREACH subway_xia1_grp GENERATE FLATTEN(group) AS (date:chararray,is_holiday:chararray,station_O:chararray),(long)(COUNT(subway_final)*1.0/0.87) AS flow:long;
    -- weekday + weekend + holida
subway_xia_grp = GROUP subway_xia1_gen BY (is_holiday,station_O);
subway_xia_gen = FOREACH subway_xia_grp GENERATE FLATTEN(group) AS (is_holiday:chararray,station_O:chararray),(long)AVG(subway_xia1_gen.flow) AS avg_num:long;

data_join = JOIN subway_shang_gen BY (is_holiday,station_O),subway_xia_gen BY (is_holiday,station_O);
data_join_gen = FOREACH data_join GENERATE $0 AS is_holiday:chararray,$1 AS station_O:chararray,$2 AS in_flow:long,$6 AS out_flow:long,($2+$6) as total_avg:long;


subway_station_allday_in_out_flow = ORDER data_join_gen BY total_avg DESC ;



--morning/evening rush

SPLIT subway_final INTO subway_final1_mor IF (timeME(SUBSTRING(o_time, 11, 19),'$am_start') AND timeL(SUBSTRING(o_time, 11, 19),'$am_end')),
subway_final1_eve IF (timeME(SUBSTRING(o_time, 11, 19),'$pm_start') AND timeL(SUBSTRING(o_time, 11, 19),'$pm_end'));

subway_final_mor = FOREACH subway_final1_mor GENERATE card_id,o_time,station_name1,date,is_holiday,changetime,'mor' AS period:chararray;
subway_final_eve = FOREACH subway_final1_eve GENERATE card_id,o_time,station_name1,date,is_holiday,changetime,'eve' AS period:chararray;

subway_final2 = UNION subway_final_mor,subway_final_eve;


subway_everyday_grp = GROUP subway_final2 BY (date,is_holiday,station_name1,period);
subway_everyday_gen = FOREACH subway_everyday_grp GENERATE FLATTEN(group) AS (date:chararray,is_holiday:chararray,station_name1:chararray,period:chararray),(long)(COUNT(subway_final2)*1.0/0.87) AS num:long;


-- passenger volume for each original-destination (OD) per month
  -- weekday + weekend + holida
subway_yuejun_grp = GROUP subway_everyday_gen BY (is_holiday,station_name1,period);
subway_yuejun_gen = FOREACH subway_yuejun_grp GENERATE FLATTEN(group) AS (is_holiday:chararray,station_name1:chararray,period:chararray),(long)AVG(subway_everyday_gen.num) AS avg_num:long;

    --top10
subway_yuejun_ord_grp = GROUP subway_yuejun_gen BY (is_holiday,period);
subway_station_mor_eve_in_flow_topi = FOREACH subway_yuejun_ord_grp {
	sort = ORDER subway_yuejun_gen BY avg_num DESC ;
	top10 = LIMIT sort $i;
	GENERATE FLATTEN(top10) AS (is_holiday:chararray,station_name1:chararray,period:chararray,avg_num:long);
};
