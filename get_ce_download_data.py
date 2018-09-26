#!/usr/bin/python

import os, time, sys

path = '/theHoard/export/stuff/DailyDwnldCsvFiles/'
last = os.path.getmtime('./last_execution')
suffix = '.csv'

commandline = "ssh centos@52.204.218.249 /home/centos/daily_download/cleanup.sh"
os.system(commandline)

for f in os.listdir(path):
  fullfile = os.path.join(path,f)
  if os.stat(fullfile).st_mtime > last:
    if os.path.isfile(fullfile) and fullfile.endswith(suffix):
      print fullfile
      commandline = "scp " +fullfile +" " +"centos@52.204.218.249:~/daily_download/data/"
      os.system(commandline)
      now = time.time()
      os.utime('./last_execution', (now,now))

commandline = "ssh centos@52.204.218.249 /home/centos/daily_download/daily.py"
os.system(commandline)
