 # -*- coding: UTF-8 -*-

import common
import traceback

if __name__=='__main__':
    #db = common.get_mongodb_conn('10.247.32.100',27021,'ab9fd8f1-33d3-11e7-8297-00163e020112','6d6dd9520aa5788970fa8587ea571edb','b6e7cd527b6ff1187285b4a82a1a3019',collection)
    gp = common.get_greenplum_conn('10.247.32.84','5432','d18e6e703f0dcfa4','u3aae3921f2ee6cc','pd83c000136e3436')
    #sql_create1 = "CREATE TABLE sensor_count(date varchar(32) NOT NULL,deviceid varchar(32) NOT NULL,sensor varchar(32) NOT NULL,count integer NOT NULL, primary key(date, deviceid, sensor));"
    #test = "insert into sensor_count(date, deviceid, sensor, count) values ('2017-06-18','073b5e4aae904f67ac1d1db7285a0bfb','L0085','0')"


    #sql_create1 = "CREATE TABLE limo_hour_max_min(date varchar(32) NOT NULL, hour integer NOT NULL, deviceid varchar(32) NOT NULL, sensor varchar(32) NOT NULL, max float NOT NULL, min float NOT NULL, avg float NOT NULL, primary key(date, hour, deviceid, sensor))"
    #sql_create1 = "CREATE TABLE sensor_restart_count(date varchar(32) NOT NULL,deviceid varchar(32) NOT NULL, restart_count float NOT NULL, spendtime float NOT NULL, primary key(date, deviceid))"
    #sql_create3 = "CREATE TABLE sensor_temperature_kmeans(date varchar(32) NOT NULL, clusterid integer NOT NULL, cluster_num integer NOT NULL,sensor1 float NOT NULL, sensor2 float NOT NULL, sensor3 float NOT NULL, primary key(date, clusterid))"
    #sql_create1 = "CREATE TABLE sensor_exceed(date varchar(32) NOT NULL, deviceid varchar(32) NOT NULL, sensor varchar(32) NOT NULL, hour integer NOT NULL, count integer NOT NULL, primary key(date, deviceid,sensor, hour ) )"
    sql_create1 = "CREATE TABLE limo_max_min(date varchar(32) NOT NULL,deviceid varchar(32) NOT NULL,sensor varchar(32) NOT NULL,max float NOT NULL, min float NOT NULL,avg float NOT NULL, primary key(date, deviceid, sensor))"
    #sql_create4 = "CREATE TABLE sensor_top_value(date varchar(32) NOT NULL, deviceid varchar(32) NOT NULL, sensor varchar(32) NOT NULL, topvalue integer NOT NULL, count integer NOT NULL,primary key(date, deviceid, sensor) )"
    #sql_create1 = "CREATE TABLE  sensor_device_type(deviceid varchar(32) NOT NULL,type varchar(32) NOT NULL,  primary key(deviceid))"
    #sql_create1 = "CREATE TABLE  sensor_boxplot(date varchar(32) NOT NULL,sensor varchar(32) NOT NULL,  lower integer NOT NULL,  Q1 integer NOT NULL, median integer NOT NULL, Q3 integer NOT NULL, upper integer NOT NULL, primary key(date,sensor))"
    #sql_create1 = "CREATE TABLE  sensor_outliers(date varchar(32) NOT NULL,sensor varchar(32) NOT NULL,  measure integer NOT NULL)"
    #sql_create1 = "CREATE TABLE  sensor_matrix(date varchar(32) NOT NULL,L0013 float NOT NULL,L0014 float NOT NULL,L0023 float NOT NULL,L0032 float NOT NULL,L0033 float NOT NULL,L0041 float NOT NULL,"+\
    #"L0042 float NOT NULL,L0048 float NOT NULL,L0067 float NOT NULL,L0075 float NOT NULL,L0080 float NOT NULL,L0090 float NOT NULL,L0092 float NOT NULL)"
    try:
      #gp[1].execute(sql_create1)
      gp[1].execute(sql_create1)
      gp[0].commit()
    except:
      traceback.print_exc()
      pass
