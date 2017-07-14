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


sensor_list = ["L0067","L0075","L0080"]



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
data = sc.textFile("file:////Users/zhuangzhuanghuang/Downloads/data/dfwfc-2017-07-07.csv")

header = data.take(1)[0]
rdd = data.filter(lambda line: line != header).map(parseLine)
parsedData = rdd.filter(lambda x: x[0] in sensor_list  ).map(lambda x: (x[0] + ',' + x[1], x[2])).groupByKey();
results = parsedData.mapValues(list).collect()

result_list1 = len(results[0][1])
results_list2 = len(results[1][1])
results_list3 = len(results[2][1])

min_len = min(result_list1,results_list2,results_list3);



results_list = [[],[],[]]
results_list[0] = list(results[0][1])[:(min_len-1)]
results_list[1] = list(results[1][1])[:(min_len-1)]
results_list[2] = list(results[2][1])[:(min_len-1)]



datenow = (results[0][0].split(','))[1]

mat = sc.parallelize(np.column_stack(results_list));
#summary = Statistics.colStats(mat)
#print(summary.mean())
clusters = KMeans.train(mat, 3, maxIterations=10)
transformeds = clusters.predict(mat).collect()

first_group, second_group, third_group = 0,0,0
for transformed in  transformeds:
    predict_value = int(transformed)
    if(predict_value==0):
        first_group = first_group+1
    if(predict_value==1):
        second_group = second_group+1
    if(predict_value==2):
        third_group = third_group+1

cluster_nums = [first_group, first_group,third_group]


print("{} length of trans".format(len(transformeds)))

cluster_list = [];

time =0;
#print(str(clusters.countByValue().items()))
for cluster in clusters.clusterCenters:
    sensor1 = float(cluster[0])
    sensor2 = float(cluster[1])
    sensor3 = float(cluster[2])
    cluster_num = cluster_nums[time]
    result_tuple = (datenow,time,cluster_num,sensor1, sensor2,sensor3)
    cluster_list.append(result_tuple);
    time = time+1;


df = sqlContext.createDataFrame(cluster_list, ["date", "clusterid","cluster_num","sensor1", "sensor2","sensor3"])
df.write.jdbc(url=url, table="sensor_temperature_kmeans", mode="append", properties=properties)

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
