#!/usr/bin/env python
# coding: utf-8
import time
import daemon
import logbook
import datetime
import os,sys,re
import logging
import requests
from dbs import RedisDB
from dbs import ESDb
from ftp import MyFtp
from pyhdfs import HdfsClient
from multiprocessing import Pool
from werkzeug import secure_filename
from ConfigParser import ConfigParser
from logbook.more import ColorizedStderrHandler
from setproctitle import setproctitle,getproctitle
from flask import Flask,request,render_template,redirect,url_for
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

Sdir = os.path.split(os.path.realpath(sys.argv[0]))[0]
os.chdir(Sdir)

version = 20181025
config = ConfigParser()
config.read('config.ini')

filepath = os.path.abspath(sys.argv[0])
filename = filepath.split('/')[-1]
procname = filename.split('.')[0]
setproctitle(procname)

Usage = """Usage:
    %s [COMMAND]

Commands:
    help       -- help about any command
    task       -- show task list
    add [YYYYmmdd/HHMM]   -- Add a task
    jobs list <-n number>	-- show Task completion list,default 10 number
    version     -- show version 
""" % procname

HDFS = config.get('Log','Hdfs')
HdfsPath = config.get('Log','HdfsPath')
ES = config.get('Log','ES')
Es_Type = config.get('Log','EStype')
Index = config.get('Log','Index')
Mins = config.get('Log','Mins')
TimeOut = config.get('Log','TimeOut')
Customer = config.get('Log','Customer')
## Redis
Rhost = config.get('REDIS','HOST')
RPASSWD = config.get('REDIS','PASSWD')
Rport = config.get('REDIS','PORT')
RDB = config.get('REDIS','DB')
Rkey = config.get('REDIS','HKEY')

Domains = config.get('DOMAIN','DOMAINS').split(' ')

APP_NAME = ':'
LOG_DIR = os.path.join('log')
LOG_PATH = os.path.join(LOG_DIR,'run.log')

def get_logger(name=APP_NAME, file_log=False):
    logbook.set_datetime_format('local')
    ## 是否输出到sys.stdout
    ColorizedStderrHandler(bubble=False).push_application()
    if file_log:
        logbook.TimedRotatingFileHandler(LOG_PATH, date_format='%Y%m%d', bubble=True).push_application()
    return logbook.Logger(name)

app = Flask(__name__)
log = logging.getLogger('werkzeug')
log.setLevel(logging.WARN)

class LogTime():
    def __init__(self, jobTime):
        self.jobTime = jobTime
        self.jobTimeObj = time.strptime(self.jobTime, '%Y%m%d/%H%M')
        self.PathDay = time.strftime('%Y%m%d', self.jobTimeObj)
        self.Path_Day = time.strftime('%Y_%m_%d', self.jobTimeObj)
        self.ObjTime = time.strftime('%H%M', self.jobTimeObj)
        self.success = "_SUCCESS"

    ## hdfs 目录格式
    @property
    def HPath(self):
        return "%s/%s/%s/" % (HdfsPath, self.PathDay, self.ObjTime)

    ## hdfs 文件格式 2018_10_19_1200.gz
    @property
    def LogName(self):
        return "_%s_%s.gz" % (self.Path_Day, self.ObjTime)

    ## 定义客户日志上传格式 20181019_1200.gz
    @property
    def UpFile(self):
        return "_%s_%s.gz" % (self.PathDay, self.ObjTime)

    def UpTmp(self):
        return "_%s_%s.gz.tmp" % (self.PathDay, self.ObjTime)

def Upload(remote,local,days):
    try:
        myftp = MyFtp()
        myftp.dirs(days)
        myftp.Upload(remote,local)
    except Exception,error:
        logger.error(error)
    myftp.ftp.quit()

def Rename(remote):
    try:
        myftp = MyFtp()
        myftp.Rename(remote)
    except Exception,error:
        logger.error(error)
    myftp.ftp.quit()

def Timeout_log(jobTime):
    nowTime = datetime.datetime.now()
    newTs = int(time.mktime(nowTime.timetuple()))
    jobTimeTs = int(time.mktime(time.strptime(jobTime,"%Y%m%d/%H%M")))
    return (TimeOut < (newTs - jobTimeTs))

def GetJobs():
    handlejobs = r.rgall(Rkey)
    jobs = []
    try:
        for handlejob in handlejobs:
            if Timeout_log(handlejob):
                logger.warn((handlejob + " Timeout Delete Key"))
                r.rdel(Rkey, handlejob)
                continue
            if handlejobs[handlejob] == "1":
                continue
            jobs.append(handlejob)
        return jobs
    except Exception,e:
        logger.error(e)

def Run(jobTime):
    Client = HdfsClient(hosts=HDFS)
    Log = LogTime(jobTime)
    if Client.exists(Log.HPath + Log.success):
        r.rset(Rkey,jobTime,1)
        logger.info("-------- %s -----------" % jobTime)
        for dm in Domains:
            Files = Log.HPath + dm + Log.LogName
            if Client.exists(Files):
                Dfile = DOWN_DIR + '/' + dm + Log.UpFile
                TmpFile = dm + Log.UpTmp()
                try:
                    sts = time.time()
                    logger.info('DownloadStart... %s' % Files)
                    Client.copy_to_local(Files,Dfile)
                    logger.info('DownloadSuccess... %s %s' % (Files,Dfile))
                    Upload(TmpFile,Dfile,Log.PathDay)
                    Rename(Dday + "/" + TmpFile)
                    logger.info('UploadSuccess... %s' % Dfile)
                    ets = time.time()
                    Write(jobTime,dm,sts,ets,200)
                    r.rdel(Rkey,jobTime)
                except Exception,e:
                    Write(jobTime,dm,500)
                    r.rdel(Rkey,jobTime)
                    logger.error(e)
            else:
                logger.warn(Files + ' Non-existent')
                continue
        r.rdel(Rkey,jobTime)
    else:
        logger.warn(Log.HPath + " _SUCCESS Non-existent")

def Write(jobTime,dm,sts,ets,status):
	es = ESDb(ES)
	timestamp = int(time.mktime(time.strptime(jobTime, "%Y%m%d/%H%M")))
	days = time.strftime("%Y.%m.%d", time.localtime(timestamp))
	index = '%s-%s' % (Index, days)
	Log = LogTime(jobTime)
	file = "%s%s" % (dm,Log.UpFile)
	es_type = Es_Type
        ts = timestamp - 28800
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.localtime(ts))
	stime = time.strftime("%Y%m%d %H:%M:%S", time.localtime(sts))
	etime = time.strftime("%Y%m%d %H:%M:%S", time.localtime(ets))
	stss = int(str(sts).split('.')[0])
	etss = int(str(ets).split('.')[0])
	rtime = etss - stss
	body = { "log_type": es_type,
		"customer": Customer,
		"dm": dm,
		"logfile": file,
		"stime": stime,
		"etime": etime,
		"rts": rtime,
        "jobs": jobTime,
		"@timestamp": timestamp,
		"status": status
	}
	if not es.Index(index,es_type):
            url = "%s/%s" % (ES,index)
            response = requests.request("PUT", url)
	es.Create(index, es_type, body)

def main():
	Times = (datetime.datetime.now() - datetime.timedelta(minutes = int(Mins)))
	Timestamp = time.mktime(Times.timetuple())
	ts = int(str(Timestamp).split('.')[0])
	jobTime = time.strftime("%Y%m%d/%H%M", time.localtime(ts))
        r.rset(Rkey, jobTime, 0)
	jobs = GetJobs()
	pool = Pool()
	pool.map(Run,jobs)
	pool.close()
	pool.join()

def work(func):
	scheduler = BackgroundScheduler()
#	trigger = IntervalTrigger(minute=5)
   	trigger = CronTrigger(minute='*/5')
	#scheduler.add_job(func,'cron',minute='*/5')
	scheduler.add_job(func,trigger)
	scheduler.start()
	logger.info('Start ...')
	# try:
	# 	while True:
	# 		time.sleep(2)
	# except (KeyboardInterrupt, SystemExit):
	# 	scheduler.shutdown()
	# 	logger.info('End ...')

@app.route('/ok',methods=['GET'])
def oks():
	return 'ok'

@app.route('/add/<jobs>',methods=['POST'])
def addjob(jobs):
    if re.match('2([0-9]{7}):([0-9]{4})', jobs):
        jobTime = jobs.replace(':','/')
        r.rset(Rkey,jobTime,0)
        return jobTime
    else:
        return "参数错误"

@app.route('/api/del/<jobs>',methods=['POST'])
def deljob(jobs):
	if re.match('2([0-9]{7}):([0-9]{4})',jobs):
		jobTime = jobs.replace(':', '/')
		if r.rvalue(Rkey,jobTime):
			r.rdel(Rkey,jobTime)
			return jobTime
		else:
			return "None"
	else:
		return '{"error":"ture"}'

@app.route('/api/ps',methods=['GET'])
def ps():
	return r.glist(Rkey)

@app.route('/api/<index>/<num>',methods=['GET'])
def List(index,num):
	es = ESDb(ES)
	return es.Search(index,num)

if __name__ == '__main__':
    r = RedisDB(Rhost, RPASSWD, Rport, RDB)
    argv = len(sys.argv) - 1
    if argv >= 1:
        if argv == 1:
            if sys.argv[1] == 'task':
                print r.glist(Rkey)
                sys.exit()
            elif sys.argv[1] == 'help':
                print Usage
                sys.exit()
            elif sys.argv[1] == 'version':
                print version
                sys.exit()
            else:
                print Usage
                sys.exit()
        elif argv == 2:
            if sys.argv[1] == 'add' and sys.argv[2]:
                jobs = sys.argv[2]
                if re.match('\d{8}/\d{4}$', jobs):
                    r.rset(Rkey, jobs, 0)
                    print r.glist(Rkey)
                    sys.exit()
                else:
                    print Usage
                    sys.exit()
            elif sys.argv[1] == 'jobs' and sys.argv[2] == 'list':
                print "jobs list"
                sys.exit()
            else:
                print Usage
                sys.exit()
        elif argv == 4:
            num = sys.argv[4]
            if sys.argv[1] == 'jobs' and sys.argv[2] == 'list' and sys.argv[3] == '-n' and re.match('[1-9]\d*', num):
                print "jobs list -n %s" % num
                sys.exit()
            else:
                print Usage
                sys.exit()
        else:
            print Usage
            sys.exit()
    logger = get_logger(file_log=True)
    try:
        Times = (datetime.datetime.now() - datetime.timedelta(minutes=int(Mins)))
        Dday = Times.strftime("%Y%m%d")

        DOWN_DIR = os.path.join('cdn/' + Dday)
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)

        if not os.path.exists(DOWN_DIR):
            os.makedirs(DOWN_DIR)

        work(main)
        app.run(host='0.0.0.0',port=5901,debug=False)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info('End ...')
