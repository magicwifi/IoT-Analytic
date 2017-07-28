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

sensor_list = ["L0091","L0080"]



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


conf = SparkConf().setMaster("local[*]").setAppName("dfwfc1")
sc = SparkContext(conf = conf)



sqlContext = SQLContext(sc)

url = 'jdbc:postgresql://10.247.32.84:5432/d18e6e703f0dcfa4'
properties = {'user': 'u3aae3921f2ee6cc', 'password': 'pd83c000136e3436'}

datenow = datetime.datetime.now().strftime('%Y-%m-%d')
data = sc.textFile("file:////Users/zhuangzhuanghuang/Downloads/data/dfwfc-"+str(sys.argv[1])+".csv")

header = data.take(1)[0]
rdd = data.filter(lambda line: line != header).map(parseLine)
parsedData = rdd.filter(lambda x: x[0] in sensor_list  ).map(lambda x: (x[0] + ',' + x[1], x[2])).groupByKey();
results = parsedData.mapValues(list).collect()

result_list1 = len(results[0][1])
results_list2 = len(results[1][1])


min_len = min(result_list1,results_list2);


temp1 = list(results[0][1])[:(min_len-1)]
temp2 = list(results[1][1])[:(min_len-1)]

x= sc.parallelize(temp1,2)
y= sc.parallelize(temp2,2)

result1 = Statistics.corr(x, y, "spearman")

print("test1 {}".format(result1))

result2 = Statistics.corr(x, y)

print("test2 {}".format(result2))





#print("Final centers: " + str(clusters.clusterCenters))


#averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])


# Load and parse the data
#data = sc.textFile("file:////Users/zhuangzhuanghuang/Downloads/dfwfc1.csv")
#parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))

# Build the model (cluster the data)
#kmeans = KMeans().setK(2).setSeed(1)
#model = kmeans.fit(parsedData)

# Evaluate clustering by computing Within Set Sum of Squared Errors.
#wssse = model.computeCost(parsedData)
#print("Within Set Sum of Squared Errors = " + str(wssse))

# Shows the result.
#centers = model.clusterCenters()
#print("Cluster Centers: ")
#for center in centers:
    #print(center)
