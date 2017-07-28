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

sensor_list = ["L0013","L0014","L0023","L0032","L0033","L0041","L0042","L0048","L0067","L0075","L0080","L0090","L0092"]



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
parsedData = rdd.filter(lambda x: x[0] in sensor_list  ).map(lambda x: (x[0] + ',' + x[1], x[2])).groupByKey().cache().sortByKey(True, 1);
results = parsedData.mapValues(list).collect()

results_list1 = len(results[0][1])
results_list2 = len(results[1][1])
results_list3 = len(results[2][1])
results_list4 = len(results[3][1])
results_list5 = len(results[4][1])
results_list6 = len(results[5][1])
results_list7 = len(results[6][1])
results_list8 = len(results[7][1])
results_list9 = len(results[8][1])
results_list10 = len(results[9][1])
results_list11 = len(results[10][1])
results_list12 = len(results[11][1])
results_list13 = len(results[12][1])



min_len = min(results_list1,results_list2,results_list3,results_list4,results_list5,results_list6,results_list7,results_list8,results_list9, results_list10,results_list11,results_list12  );



results_list = [[],[],[],[],[],[],[],[],[],[],[],[],[]]
results_list[0] = list(results[0][1])[:(min_len-1)]
results_list[1] = list(results[1][1])[:(min_len-1)]
results_list[2] = list(results[2][1])[:(min_len-1)]
results_list[3] = list(results[3][1])[:(min_len-1)]
results_list[4] = list(results[4][1])[:(min_len-1)]
results_list[5] = list(results[5][1])[:(min_len-1)]
results_list[6] = list(results[6][1])[:(min_len-1)]
results_list[7] = list(results[7][1])[:(min_len-1)]
results_list[8] = list(results[8][1])[:(min_len-1)]
results_list[9] = list(results[9][1])[:(min_len-1)]
results_list[10] = list(results[10][1])[:(min_len-1)]
results_list[11] = list(results[11][1])[:(min_len-1)]
results_list[12] = list(results[12][1])[:(min_len-1)]




mat = sc.parallelize(np.column_stack(results_list));
#summary = Statistics.colStats(mat)
#print(summary.mean())

pearsonCorr  = Statistics.corr(mat, method="pearson")


result_lists = []



for result in pearsonCorr:
    date1 =  str(sys.argv[1])
    L13 = float(result[0])
    L14 = float(result[1])
    L23 = float(result[2])
    L32 = float(result[3])
    L33 = float(result[4])
    L41 = float(result[5])
    L42 = float(result[6])
    L48 = float(result[7])
    L67 = float(result[8])
    L75 = float(result[9])
    L80 = float(result[10])
    L90 = float(result[11])
    L92 = float(result[12])
    result_tuple = (date1, L13, L14, L23, L32, L33, L41, L42, L48, L67, L75,L80,L90, L92)
    result_lists.append(result_tuple)

df = sqlContext.createDataFrame(result_lists, ["date","l0013","l0014","l0023","l0032","l0033","l0041","l0042","l0048","l0067","l0075","l0080","l0090","l0092"] )


df.write.jdbc(url=url, table="sensor_matrix", mode="append", properties=properties)




