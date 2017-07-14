 # -*- coding: UTF-8 -*-

from pymongo import MongoClient
import psycopg2
from datetime import date,timedelta
import traceback
import time
import re
import datetime


U = 10000
I_sensor = 'L0059'
sensor_list = ['L0003','L0010','L0013','L0014','L0023','L0027','L0032','L0033','L0035',
               'L0041','L0042','L0048','L0056','L0065','L0067','L0070','L0072','L0075',
               'L0076','L0077','L0079','L0080','L0082','L0090','L0092','L0094']


#建立 mongodb 连接
def get_mongodb_conn(ip,port,dbname,user,pwd,collection):
    client = MongoClient(ip,port)
    mongodb_conn = client[dbname]
    mongodb_conn.authenticate(user,pwd)
    return mongodb_conn

#建立greenplum 连接
def get_greenplum_conn(ip,port,dbname,user,pwd):
    gp_conn = psycopg2.connect(database=dbname, user=user, password=pwd, host=ip, port=port)
    gp_cur = gp_conn.cursor()
    gp_link = [gp_conn,gp_cur]
    return gp_link

#获取 mongodb 中数据的timespan （该collection数据时间跨度：2017-06-16 16:26:50 至 2017-06-22 12:02:25）
def get_timespan(gp_cur,gp_tablename):
    sql_select_date = "select distinct substr(timestamp,0,11) as date from "+gp_tablename+" order by date"
    gp_cur.execute(sql_select_date)
    date_info = gp_cur.fetchall()
    date_count = len(date_info)
    datelist = []
    timespanlist = []
    for record in date_info:
        date = record[0]
        date = ''.join(re.findall('[0-9]+',date))
        datelist.append(date)
    for i in range(date_count):
        j=i
        tmp_date = datetime.strptime(datelist[i],'%Y%m%d') - timedelta(days=1)
        timespan = [tmp_date.strftime('%Y%m%d')+' 16:00:00', datelist[i]+' 16:00:00']
        timespanlist.append(timespan)
    return datelist,timespanlist

#mongodb数据解析清洗装载到GP
def load_mongo_data_to_gp(mongo_conn,tablename,timespan,gp_link):
    result = mongo_conn.tablename.find({"timestamp":{"$gte":datetime.datetime(2017, 6, 30, 0),"$lt":datetime.datetime(2017, 7, 1, 0)}})
    #result = mongo_conn.E_hoister.find({"timestamp":{"$lt":datetime.utcnow()}})
    gp_tablename = tablename+"_trans1"
    count = 0
    for record in result:
        if(record['sensor'] == 'test'):
            pass
        else:
            sql_insert = "insert into "+ gp_tablename +" (deviceid,topic,sensor,measure,timestamp) values(%s,%s,%s,%s,%s)"
            #写入GP
            try:
                gp_link[1].execute(sql_insert,( record['deviceid'],
                                      record['topic'],
                                      record['sensor'],
                                      record['measure'],
                                      record['timestamp']))
                count = count+1
                while(count == 5000):
                  gp_link[0].commit()
                  count = 0
            except:
                traceback.print_exc()
                pass
    gp_link[0].commit()
    return gp_tablename

#GP按照设备拆分建立临时表并装载数据
def pre_gp_device_table(gp_link,tablename,datelist,timespanlist):
    devicenamelist = []
    talenamelist = []
    #1、按照设备建立临时表,命名格式：dev_设备名_加工日期_tmp,并将数据按照设备名分别插入对应表
    gp_link[1].execute("select deviceid,count(*) from "+ tablename +" group by deviceid;")
    device_info = gp_link[1].fetchall()
    device_count = len(device_info)
    for i in range(len(datelist)):
     for row in device_info:
        devicename =row[0]
        new_tablename = "dev_"+devicename+"_"+datelist[i]+"_tmp"
        #devicenamelist.append(devicename)
        talenamelist.append(new_tablename)
        sql_create_table = "CREATE TABLE "+new_tablename+" (deviceid varchar(32) NOT NULL,measure float, timestamp timestamp,sensor varchar(32));"
        sql_insert = "insert into "+new_tablename+" select deviceid,measure,timestamp,sensor from "+ tablename+" where deviceid = '%s'and timestamp between '%s' and  '%s' order by timestamp ;"\
                     %(devicename,timespanlist[i][0],timespanlist[i][1])
        try:
            #gp_link[1].execute(sql_create_table)
            #gp_link[0].commit()
            #gp_link[1].execute(sql_insert)
            gp_link[0].commit()
        except:
            traceback.print_exc()
            pass
    return talenamelist

#统计某台设备的运行时长,累计运行时长，启停次数，用电量，累计用电量等
def gp_date_etl(gp_link,tablename,data,data_count):
    tmp_count = 0 #记录设备记录数
    invalid_count = 0 #记录设备未启动记录数
    j = 0
    starttime=''
    stoptime=''
    runtime = 0
    total_runtime = 0
    avg_I = 0
    sum_I = 0
    kwh = 0
    total_kwh = 0
    devicename = tablename.split('_')[1]  #"dev_"+devicename+"_"+datelist[i]+"_tmp"，获取设备名
    static_date = tablename.split('_')[2]  #汇总表中日期取数据日期
    #获取并判断设备状态并记录起、止时间
    for result in data:
            if(result[1]!=0 and tmp_count == 0):#第一条设备启动记录
               starttime = result[0] #记录设备单次启动时间
               tmp_count=tmp_count+1
            elif(result[1]==0 and tmp_count == 0):#第一条记录以及连续都是设备停止状态，不做任何处理
                invalid_count=invalid_count+1
                pass
            elif(result[1]!=0 and tmp_count > 0 and tmp_count < data_count-invalid_count-1):#设备启动状态中的数据
                tmp_count=tmp_count+1
            elif(result[1]!=0 and tmp_count == data_count-invalid_count-1):#设备启动状态,但该数据是最后一条数据，即该设备一直在运行,启动状态的运行时间差作为运行时长
                stoptime = result[0]
                tmp_count=tmp_count+1
            else:#记录第一条设备停止数据
                stoptime = result[0]#记录设备单次停止时间
                j+=1
            if(starttime != '' and  stoptime != '' and tmp_count != 0):
              starttimeStamp = int(time.mktime(starttime.timetuple()))
              stoptimeStamp = int(time.mktime(stoptime.timetuple()))
              runtime = stoptimeStamp - starttimeStamp #计算单次运行时长
             #获取设备本次运行期间的电流
              sql_select_I = "select measure from "+tablename+" where sensor = '"+ I_sensor +"' and timestamp between '%s' and  '%s' order by timestamp;" %(starttime,stoptime)
              gp_link[1].execute(sql_select_I)
              I_data = gp_link[1].fetchall() #timestamp,
              I_data_count = len(I_data)
              for ret in I_data:
                 sum_I += ret[0]
              avg_I = sum_I/I_data_count
              #计算设备该运行时段内的用电量,设备运行期间的电压恒定，U = 10000
              kwh=round(U*avg_I*runtime/(1000*3600),2)
              #output_device_status表：日期，设备，启动时间，停机时间，用电量
              sql_insert = "insert into etl_device_status_info(date,deviceid,starttime,stoptime,kwh) values(%s,%s,%s,%s,%s);"
              try:
                gp_link[1].execute(sql_insert,(static_date,devicename,starttime,stoptime,kwh))
                gp_link[0].commit()
                tmp_count = 0
                invalid_count = 0
                starttime=''
                stoptime=''
                avg_I = 0
                sum_I = 0
              except:
                traceback.print_exc()
                pass
    if(starttime == '' and stoptime == '' and kwh == 0): #设备一直未启动
            sql_insert_zero = "insert into etl_device_status_info(date,deviceid,kwh) values(%s,%s,%s);"
            gp_link[1].execute(sql_insert_zero,(static_date,devicename,kwh))
            gp_link[0].commit()
    stop_count = j #统计设备启停几次
    total_runtime+=runtime#计算累计运行时长
    total_kwh+=kwh#计算累计用电量
    #output_statistic表：日期，设备，启停次数，累计时长，累计电量
    sql_insert = "insert into etl_device_statistic(date,deviceid,restart_count,total_runtime,total_kwh) values(%s,%s,%s,%s,%s);"
    gp_link[1].execute(sql_insert,(static_date,devicename,stop_count,total_runtime,total_kwh))
    gp_link[0].commit()


