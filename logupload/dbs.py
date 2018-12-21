#!/usr/bin/env python
# coding: utf-8
import redis
import sys
import json
import mysql.connector
from elasticsearch import Elasticsearch

class MysqlDB():
    def __init__(self,User,Host,Password,DB):
        self.db = mysql.connector.connect(user=User,host=Host,password=Password,database=DB)
        self.cursor  = self.db.cursor()

    def Exec(self,Sqls):
        self.cursor.execute(Sqls)
        self.db.commit()
        self.db.close()

    def Value(self,Sqls):
        self.cursor.execute(Sqls)
        return self.cursor.fetchall()
        self.db.close()


class RedisDB():
    def __init__(self,host,password,port,db):
        try:
            self.pool = redis.ConnectionPool(host=host, password=password, port=port, db=db, max_connections=10)
            self.RedisClient = redis.Redis(connection_pool=self.pool)
        except Exception, error:
            return error
            raise Exception(error)
            sys.exit()

    def rset(self, Rkey, domain, values):
        self.RedisClient.hset(Rkey, domain, values)
        self.RedisClient.shutdown

    def rgall(self,Rkey):
        ''' 获取所有队列'''
        return self.RedisClient.hgetall(Rkey)

    def glist(self,Rkey):
        values = self.RedisClient.hgetall(Rkey)
        # return "%-20s%-20s" % ('Times', 'Status')
        code = {}
        for i in values.keys():
            sts = values.get(i)
            if int(sts) == 1:
                status = 'running'
            else:
                status = "waiting"
            # return "%-20s%-20s" % (i,status)
            code[i] = status
        jsonStr = json.dumps(code)
        return jsonStr

    def rget(self,Rkey):
        return self.RedisClient.hkeys(Rkey)
        self.RedisClient.shutdown

    def rdel(self,Rkey,domain):
        self.RedisClient.hdel(Rkey, domain)
        self.RedisClient.shutdown

    def rvalue(self,Rkey,Key):
        return self.RedisClient.hget(Rkey,Key)
        self.RedisClient.shutdown

    @property
    def down(self):
        self.RedisClient.shutdown

class ESDb():
    def __init__(self,host):
        self.es = Elasticsearch(host)

    def Index(self,index,type):
        try:
            self.es.cat.indices(index)
        except:
            # self.es.index(index,type,body={})
            return False

    def Create(self,index,types,body):
        self.es.index(index,doc_type=types,body=body)

    def Search(self,index,num=10):
        body = {
            "from": 0,
            "size": num
        }
        values = self.es.search(index=index,body=body)
        data = len(values.get('hits').get('hits'))
        SerList = []
        # print "%-25s%-20s%25s" % ('域名','文件','状态')
        for i in range(data):
            code = {}
            a = values.get('hits').get('hits')[i].get('_source')
            dm = a.get('dm')
            logs = a.get('logfile')
            sts = a.get('status')
            code['domain'] = dm
            code['logfile'] = logs
            code['status'] = sts
            SerList.append(code)
            # print "%-20s%-20s%10s" % (dm,logs,sts)
        return json.dumps(SerList)

    def dm(self,index,dm,ts):
        body = {"query": {"bool": {"filter": [{"bool": {"must": [{"bool": {"must": [{"match_phrase": {"dm": {"query": dm, "slop": 0, "boost": 1 } } }, {"match_phrase": {"ts": {"query": ts, "slop": 0, "boost": 1 } } } ] } } ] } } ] } } }
        try:
            values = self.es.search(index=index,body=body)
            data = len(values.get('hits').get('hits'))
            print "%-25s%-20s%25s" % ('域名', '文件', '状态')
            for i in range(data):
                a = values.get('hits').get('hits')[i].get('_source')
                dm = a.get('dm')
                logs = a.get('logfile')
                sts = a.get('status')
                print "%-20s%-20s%10s" % (dm, logs, sts)
        except:
            print "索引不存在: %s" % index

    def check(self,index,dm,ts):
        body = {"query": {"bool": {"filter": [{"bool": {"must": [{"bool": {"must": [{"match_phrase": {"dm": {"query": dm, "slop": 0, "boost": 1 } } }, {"match_phrase": {"ts": {"query": ts, "slop": 0, "boost": 1 } } } ] } } ] } } ] } }, "_source": {"includes": ["COUNT"], "excludes": [] }, "aggregations": {"COUNT(*)": {"value_count": {"field": "_index"} } } }
        values = self.es.search(index=index,body=body)
        count = values.get('aggregations').get('COUNT(*)').get('value')
        return count

    def Check(self,index,cus,ts):
        body = {"query": {"bool": {"filter": [{"bool": {"must": [{"bool": {"must": [{"match_phrase": {"customer": {"query": cus, "slop": 0, "boost": 1 } } }, {"match_phrase": {"ts": {"query": ts, "slop": 0, "boost": 1 } } } ] } } ] } } ] } }, "_source": {"includes": ["COUNT"], "excludes": [] }, "aggregations": {"code": {"value_count": {"field": "_index"} } } }
        values = self.es.search(index=index, body=body)
        count = values.get('aggregations').get('code').get('value')
        return count
