from pyspark import SparkConf, SparkContext
import datetime
from datetime import timedelta

from pyspark.sql import DataFrameReader
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

import sys

sensor_list = ['L0003','L0010','L0013','L0014','L0023','L0027','L0032','L0033','L0035',
               'L0041','L0042','L0048','L0056','L0065','L0067','L0070','L0072','L0075',
               'L0076','L0077','L0079','L0080','L0082','L0090','L0092','L0091','L0094']

conf = SparkConf().setAppName("dfwfc1").setMaster("local[*]")

sc = SparkContext(conf = conf)


my_spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sqlContext = SQLContext(sc)

url = 'jdbc:postgresql://10.247.32.84:5432/d18e6e703f0dcfa4'
properties = {'user': 'u3aae3921f2ee6cc', 'password': 'pd83c000136e3436'}


def parseLine(line):
    fields = line.split(',')
    device = fields[0]
    datenowtmp = str(fields[3])[:19]
    utcdate  = datetime.datetime.strptime(datenowtmp, '%Y-%m-%dT%H:%M:%S')
    eighthour = timedelta(hours=+8)
    bjtime = utcdate+eighthour
    datenow = bjtime.strftime("%Y-%m-%d")
    sensor = fields[1]
    try:
      measure = float(fields[2])
    except ValueError:
      measure = 0
      pass
    return (datenow, device, sensor , measure)


datenow = datetime.datetime.now().strftime('%Y-%m-%d')
lines = sc.textFile("file:////Users/zhuangzhuanghuang/Downloads/data/dfwfc-"+str(sys.argv[1])+".csv")

header = lines.take(1)[0]
rdd = lines.filter(lambda line: line != header).map(parseLine)

totalByMax = rdd.map(lambda x: (x[0] + ',' + x[1]+','+x[2], x[3])).\
mapValues(lambda x:(x,x,x,1)).reduceByKey(lambda x, y: (max(x[0],y[0]), min(x[1],y[1]),(x[2]+y[2]),(x[3]+y[3]) )).cache().sortByKey(True, 1)
averagesByMax = totalByMax.mapValues(lambda x: (x[0],x[1], (x[2]/x[3])));

result_list = [];

results = averagesByMax.sortByKey(True, 1).collect();

for result in results :
    node1 = result[0].split(',')
    date1 = node1[0]
    device1 = node1[1]
    sensor1 = node1[2]
    node2 = result[1]
    max1 = result[1][0]
    min1 = result[1][1]
    avg = result[1][2]
    result_tuple = (date1,device1,sensor1,max1,min1,avg)
    #print(result_tuple)
    result_list.append(result_tuple)




df = sqlContext.createDataFrame(result_list, ["date", "deviceid", "sensor","max","min","avg"])


df.write.jdbc(url=url, table="limo_max_min", mode="append", properties=properties)




