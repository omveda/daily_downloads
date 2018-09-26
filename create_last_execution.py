#!/usr/bin/python
import time
import os

filepath = './last_execution'
open(filepath, 'a').close()
t = time.mktime(time.strptime('23.09.2018 23:59:59', '%d.%m.%Y %H:%M:%S'))
os.utime(filepath, (t,t))

