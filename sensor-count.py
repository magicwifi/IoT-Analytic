from pyspark import SparkConf, SparkContext
import datetime
from datetime import timedelta

from pyspark.sql import DataFrameReader
from pyspark.sql import SQLContext



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

datenow = datetime.datetime.now().strftime('%Y-%m-%d')
lines = sc.textFile("file:////Users/zhuangzhuanghuang/Downloads/data/dfwfc-2017-07-07.csv")

header = lines.take(1)[0]
rdd = lines.filter(lambda line: line != header).map(parseLine)

#rdd = lines.map(parseLine)
totalByMax = rdd.filter(lambda x:  x[1] in sensor_list).map(lambda x: (x[0] + ',' + x[1], x[2])).groupByKey();
results = totalByMax.mapValues(len).sortByKey(True, 1).collect()

results_list = []

for result in results:
    node1=result[0].split(',')
    date = str(node1[1])
    deviceid = str(node1[0])
    sensor = str(node1[2])
    count = int(result[1])
    result_tuple = (date, deviceid, sensor, count)
    results_list.append(result_tuple)


df = sqlContext.createDataFrame(results_list, ["date", "deviceid", "sensor","count"])
df.write.jdbc(url=url, table="sensor_count", mode="append", properties=properties)
