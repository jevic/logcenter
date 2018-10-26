#!/usr/bin/env python
# coding: utf-8

import os,sys
import ftplib
from ConfigParser import ConfigParser
from StringIO import StringIO

config = ConfigParser()
config.read('config.ini')

FtpHost = config.get('FTP','HOST')
username = config.get('FTP','USER')
passwd = config.get('FTP','PASSWD')
FtpPort = config.get('FTP','PORT')

def fn(n):
    pass

class MyFtp():
    ftp = ftplib.FTP()

    def __init__(self):
        try:
            self.bufsize = 1024
            self.ftp.connect(FtpHost, FtpPort)
            self.ftp.login(username, passwd)
        except Exception, e:
            print e

    def List(self, path=''):
        def callback(str):
            print str.split(" ")[-1]

        self.ftp.dir(path, callback)

    def dirs(self, remote):
        try:
            self.ftp.cwd(remote)
        except ftplib.error_perm:
            try:
                self.ftp.mkd(remote)
                self.ftp.cwd(remote)
            except ftplib.error_perm:
                print "U have no authority to make dir"

    def Size(self, remote):
        return self.ftp.size(remote)

    def DownFile(self, remote, local):
        fp = open(local, 'wb')
        self.ftp.retrbinary('RETR ' + remote, fp.write, self.bufsize)
        fp.close()

    def Rename(self, remote):
        self.ftp.rename(remote, str.replace(remote, ".tmp", ""))

    def Upload(self, remote, local):
        def callback(str):
            return str.split(" ")[-1]

        fp = open(local, 'rb')
        self.ftp.storbinary('STOR ' + remote, fp, self.bufsize)
        return self.ftp.dir(remote, callback)
        # self.ftp.rename(remote, str.replace(remote, ".tmp", ""))
        fp.close()