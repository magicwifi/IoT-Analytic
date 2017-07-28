# -*- coding:utf-8 -*-

from pyspark import SparkContext, SparkConf
import datetime
from datetime import timedelta
import time

from pyspark.sql import DataFrameReader
from pyspark.sql import SQLContext
import sys


stop_flag_sensor = "L0082"

appName ="data_etl_spark"
master= "local[*]"

def timeReduce(data1, data2):
	time1 = data1[0]
	time2 = data2[0]
	measure1 = float(data1[1])
	measure2 = float(data2[1])
	try:
		measure1 = float(data1[1])
		measure2 = float(data2[1])
	except ValueError:
		measure1 = 0
		measure2 = 0
		pass
	results_list =  data1[2]

	if (measure2 >0 and measure1==0):
		results_list.append(time2);
	if (measure2 ==0 and measure1 > 0 ) :
		results_list.append(time1);

	return (time2,measure2,results_list)

def parseLine(line):
    fields = line.split(',')
    datenowtmp = str(fields[3])[:19]
    #print(datenowtmp[:19])
    utcdate  = datetime.datetime.strptime(datenowtmp, '%Y-%m-%dT%H:%M:%S')
    eighthour = timedelta(hours=+8)
    bjtime = utcdate+eighthour
    datenow = bjtime.strftime("%Y-%m-%dT%H:%M:%S")
    return (fields[0], fields[1], fields[2],datenow)



conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf= conf)



sqlContext = SQLContext(sc)

url = 'jdbc:postgresql://10.247.32.84:5432/d18e6e703f0dcfa4'
properties = {'user': 'u3aae3921f2ee6cc', 'password': 'pd83c000136e3436'}



#datenow = datetime.datetime.now().strftime('%Y-%m-%d')
data = sc.textFile("file:///Users/zhuangzhuanghuang/Downloads/data/dfwfc-"+str(sys.argv[1])+".csv")

header = data.take(1)[0]
rdd = data.filter(lambda line: line != header).map(parseLine)

filter_data = rdd.filter(lambda x:  x[1] == stop_flag_sensor)\
.map(lambda x: (x[0], (x[3],x[2],[]))).reduceByKey(lambda x,y:timeReduce(x,y))

results = filter_data.sortByKey(True, 1).collect()

df_list = [];

for result in results:
	results_list = list(result[1][2])
	device = str(result[0])
	results_times = len(results_list)
	print(results_list)
	if(results_times >0):
		restart_count = int(results_times/2)
		if(results_times%2 ==0):
			sum_time = 0;
			for i in range(0,restart_count):
				datetime1 = results_list[2*i];
				time1stamps = int(time.mktime(datetime.datetime.strptime(datetime1[:19], "%Y-%m-%dT%H:%M:%S").timetuple()))
				datetime2 = results_list[2*i+1];
				time2stamps = int(time.mktime(datetime.datetime.strptime(datetime2[:19], "%Y-%m-%dT%H:%M:%S").timetuple()))
				delta_time = time2stamps - time1stamps
				sum_time += delta_time;
		if(results_times%2 == 1):
			sum_time = 0;

			for i in range(0,restart_count):
				datetime1 = results_list[2*i];
				time1stamps = int(time.mktime(datetime.datetime.strptime(datetime1[:19], "%Y-%m-%dT%H:%M:%S").timetuple()))
				datetime2 = results_list[2*i+1];
				time2stamps = int(time.mktime(datetime.datetime.strptime(datetime2[:19], "%Y-%m-%dT%H:%M:%S").timetuple()))
				delta_time = time2stamps - time1stamps
				sum_time += delta_time;
			datetime1 =  results_list[results_times-1];
			time1stamps = int(time.mktime(datetime.datetime.strptime(datetime1[:19], "%Y-%m-%dT%H:%M:%S").timetuple()))
			datetimetmp = datetime.datetime.strptime(datetime1[:10], "%Y-%m-%d")
			datetime2 =   datetimetmp + datetime.timedelta(days = 1)
			time2stamps = int(time.mktime(datetime2.timetuple()))
			delta_time = time2stamps - time1stamps
			sum_time += delta_time;
			restart_count = restart_count+1;
	elif(results_times == 0 and result[1][1] > 0 ):
		datetime1 = datetime.datetime.now()
		time1stamps = int(time.mktime(datetime1.timetuple()));
		datetime2 = datetime1 + datetime.timedelta(days = 1);
		time2stamps = int(time.mktime(datetime2.timetuple()));
		sum_time = time2stamps - time1stamps;
		restart_count = 0

	else:
		restart_count = 0
		sum_time = 0;
	yesterday = str(result[1][0])[:10]
	#yesterdate = datetime.datetime.now() - datetime.timedelta(days = 1)
	#yesterday = yesterdate.strftime("%Y-%m-%d")

	result_tuple = (yesterday, device,restart_count,sum_time)
	df_list.append(result_tuple)

print(df_list)

df = sqlContext.createDataFrame(df_list, ["date", "deviceid", "restart_count","spendtime"])
df.write.jdbc(url=url, table="sensor_restart_count", mode="append", properties=properties)

