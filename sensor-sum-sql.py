from pyspark import SparkConf, SparkContext
import datetime
from datetime import timedelta

from pyspark.sql import DataFrameReader
from pyspark.sql import SQLContext
import sys

from pyspark.sql import SparkSession
from pyspark.sql import Row

import collections


conf = SparkConf().setAppName("dfwfc-count").setMaster("local[*]")

sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SparkSQL").getOrCreate()


sqlContext = SQLContext(sc)

url = 'jdbc:postgresql://10.247.32.84:5432/d18e6e703f0dcfa4'
properties = {'user': 'u3aae3921f2ee6cc', 'password': 'pd83c000136e3436'}


def parseLine(line):
    fields = line.split(',')
    datenowtmp = str(fields[3])[:19]
    utcdate  = datetime.datetime.strptime(datenowtmp, '%Y-%m-%dT%H:%M:%S')
    eighthour = timedelta(hours=+8)
    bjtime = utcdate+eighthour
    datenow = bjtime.strftime("%Y-%m-%d")
    device = fields[0]
    sensor = fields[1]
    try:
      measure = float(fields[2])
    except ValueError:
      measure = 0
      pass
    return Row(date = datenow, deviceid=device, sensor=sensor , value=measure)

datenow = datetime.datetime.now().strftime('%Y-%m-%d')
lines = sc.textFile("file:////Users/zhuangzhuanghuang/Downloads/data/dfwfc-"+str(sys.argv[1])+".csv")

header = lines.take(1)[0]
sensor = lines.filter(lambda line: line != header).map(parseLine)

schemaSensor = spark.createDataFrame(sensor).cache()
schemaSensor.createOrReplaceTempView("sensor")

#rdd = lines.map(parseLine)
#totalByMax = rdd.filter(lambda x:  x[1] in sensor_list).map(lambda x: (x[0] + ',' + x[1], x[2])).groupByKey();
#results = totalByMax.mapValues(len).sortByKey(True, 1).collect()
result_list = [];

results = spark.sql("SELECT COUNT(*) FROM sensor")

for result in results.collect():
  datenow = str(sys.argv[1])
  devicetype = "LiMo"
  result_tuple=(datenow,devicetype,result[0])
  result_list.append(result_tuple)

df = sqlContext.createDataFrame(result_list, ["date", "type", "count"] )


df.write.jdbc(url=url, table="sensor_data_count", mode="append", properties=properties)





spark.stop()
