from pyspark import SparkConf, SparkContext
import datetime
from datetime import timedelta

from pyspark.sql import DataFrameReader
from pyspark.sql import SQLContext
import sys


sensor_list = ["L0003","L0010","L0013","L0014","L0015","L0023","L0027","L0032","L0033","L0035",
               "L0041","L0042","L0045","L0049","L0059","L0068","L0070","L0073","L0075","L0078",
               "L0079","L0080","L0082","L0083","L0085","L0093","L0095"]

conf = SparkConf().setAppName("dfwfc-count").setMaster("local[*]")

sc = SparkContext(conf = conf)


sqlContext = SQLContext(sc)

url = 'jdbc:postgresql://10.247.32.84:5432/d18e6e703f0dcfa4'
properties = {'user': 'u3aae3921f2ee6cc', 'password': 'pd83c000136e3436'}


def parseLine(line):
    fields = line.split(',')
    device= fields[0]
    sensor = fields[1]
    return (device, sensor)

datenow = datetime.datetime.now().strftime('%Y-%m-%d')
lines = sc.textFile("file:////Users/zhuangzhuanghuang/Downloads/data/dfwfc-"+str(sys.argv[1])+".csv")

header = lines.take(1)[0]
rdd = lines.filter(lambda line: line != header).map(parseLine)

#rdd = lines.map(parseLine)
totalByMax = rdd.filter(lambda x:  x[1] in sensor_list).groupByKey();
results = totalByMax.mapValues(len).collect()

results_list = []

for result in results:
    deviceid = str(result[0])
    devicetype = "LiMo"
    result_tuple = (deviceid, devicetype)
    results_list.append(result_tuple)


df = sqlContext.createDataFrame(results_list, ["deviceid", "type"])
df.write.jdbc(url=url, table="sensor_device_type", mode="append", properties=properties)
