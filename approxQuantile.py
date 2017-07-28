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

import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row


sensor_flag = "L0080"


def parseLine(line):
    fields = line.split(',')

    datenowtmp = str(fields[3])[:19]
    utcdate  = datetime.datetime.strptime(datenowtmp, '%Y-%m-%dT%H:%M:%S')
    eighthour = timedelta(hours=+8)
    bjtime = utcdate+eighthour
    datenow = bjtime.strftime("%Y-%m-%d")
    #device_datenow = fields[0]+','+datenow
    sensor = fields[1]
    try:
      measure = float(fields[2])
    except ValueError:
      measure = 0
      pass
    return (sensor ,datenow, measure)

def rdd2Row(line):
    return Row(sensor = line[0] ,date = line[1], value = line[2])


conf = SparkConf().setAppName("dfwfc-count").setMaster("local[*]")

sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SparkSQL").getOrCreate()


sqlContext = SQLContext(sc)

url = 'jdbc:postgresql://10.247.32.84:5432/d18e6e703f0dcfa4'
properties = {'user': 'u3aae3921f2ee6cc', 'password': 'pd83c000136e3436'}

datenow = datetime.datetime.now().strftime('%Y-%m-%d')
data = sc.textFile("file:////Users/zhuangzhuanghuang/Downloads/data/dfwfc-"+str(sys.argv[1])+".csv")

header = data.take(1)[0]
rdd = data.filter(lambda line: line != header).map(parseLine).filter(lambda x:  x[0] == sensor_flag )
sensor = rdd.map(rdd2Row)
schemaSensor = spark.createDataFrame(sensor).cache()
schemaSensor.createOrReplaceTempView("sensor")

quantiles = schemaSensor.stat.approxQuantile("value",[0.25,0.5,0.75],0.0)
Q1 = quantiles[0]
median = quantiles[1]
Q3 = quantiles[2]
IQR = Q3 - Q1
lowerRange = Q1 - 1.5*IQR
upperRange = Q3+ 1.5*IQR
date = str(sys.argv[1])

boxplot = [(date,sensor_flag,lowerRange,Q1,median,Q3,upperRange)]
#df = sqlContext.createDataFrame(boxplot, ["date", "sensor","lower","q1","median","q3","upper"])
#df.write.jdbc(url=url, table="sensor_boxplot", mode="append", properties=properties)


outliers = schemaSensor.filter("value >"+str(upperRange))
#outliers.show()
results_list = []
for result in outliers.collect():
  result_tuple=(result[0],result[1],result[2])
  results_list.append(result_tuple)

if len(results_list)!= 0:
    df1 = sqlContext.createDataFrame(results_list, ["date", "sensor","measure"])
    df1.write.jdbc(url=url, table="sensor_outliers", mode="append", properties=properties)


# Stop the session
spark.stop()






