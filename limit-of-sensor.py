from pyspark.mllib.clustering import KMeans
from numpy import array
from math import sqrt
from pyspark import SparkConf, SparkContext
import datetime
from datetime import timedelta

import numpy as np
from pyspark.mllib.stat import Statistics

from pyspark.sql import DataFrameReader
from pyspark.sql import SQLContext




sensor_list = ['L0003','L0010','L0013','L0014','L0023','L0027','L0032','L0033','L0035',
               'L0041','L0042','L0048','L0056','L0065','L0067','L0070','L0072','L0075',
               'L0076','L0077','L0079','L0080','L0082','L0090','L0092','L0094']



def parseLine(line):
    fields = line.split(',')
    datenowtmp = str(fields[3])[:19]
    utcdate  = datetime.datetime.strptime(datenowtmp, '%Y-%m-%dT%H:%M:%S')
    eighthour = timedelta(hours=+8)
    bjtime = utcdate+eighthour
    datenow = bjtime.strftime("%Y-%m-%d")
    device_datenow = fields[0]+','+datenow
    sensor = fields[1]
    try:
      measure = float(fields[2])
    except ValueError:
      measure = 0
      pass
    return (device_datenow, sensor , measure)


conf = SparkConf().setMaster("local[*]").setAppName("dfwfc1")
sc = SparkContext(conf = conf)


sqlContext = SQLContext(sc)

url = 'jdbc:postgresql://10.247.32.84:5432/d18e6e703f0dcfa4'
properties = {'user': 'u3aae3921f2ee6cc', 'password': 'pd83c000136e3436'}



datenow = datetime.datetime.now().strftime('%Y-%m-%d')
data = sc.textFile("file:////Users/zhuangzhuanghuang/Downloads/data/dfwfc-2017-07-07.csv")

header = data.take(1)[0]
rdd = data.filter(lambda line: line != header).map(parseLine)
parsedData = rdd.filter(lambda x:  x[1] in sensor_list ).map(lambda x: (x[0] + ',' + x[1], x[2])).groupByKey();
results = parsedData.mapValues(list).collect()
results_len = parsedData.mapValues(len)


results_list = [];

for result in results:
    result_list = list(result[1])
    sorted_result = sorted(result_list, reverse=True)
    node1 = result[0].split(',')
    date1 = node1[1]
    deviceid = node1[0]
    sensor1 = node1[2]
    result_len = results_len.lookup(result[0])[0]
    top_len = int(result_len*0.001)
    topvalue = sorted_result[top_len]
    result_tuple = (date1,deviceid,sensor1,topvalue,result_len)
    results_list.append(result_tuple)




df = sqlContext.createDataFrame(results_list, ["date", "deviceid","sensor", "topvalue","count"])
df.write.jdbc(url=url, table="sensor_top_value", mode="append", properties=properties)



