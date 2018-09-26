#!/usr/bin/python
  
"""
  Module for connecting to MapD database and creating tables with the provided data.
"""

__author__ = 'veda.shankar@gmail.com (Veda Shankar)'

if __name__ == "__main__":
  import argparse
  import sys
  import string
  import csv

import os
import pandas as pd
from pymapd import connect

connection = "NONE"

# Connect to the DB
def connect_to_mapd(str_user, str_password, str_host, str_dbname):
  global connection
  connection = connect(user=str_user, password=str_password, host=str_host, dbname=str_dbname, port=443, protocol='https')
  print connection

def drop_table_mapd(table_name):
  global connection
  command = 'drop table if exists %s' % (table_name)
  print command
  connection.execute(command)

def disconnect_mapd():
  global connection
  connection.close()

# Load CSV from S3 to table using PyMapD
def load_table_mapd(table_name, csv_file, mapd_host, mapd_user):
  global connection
  table_name = table_name.replace('.', '_')
  query = 'COPY %s from \'s3://%s\' ' % (table_name, csv_file)
  print query
  connection.execute(query)
  print connection.get_table_details(table_name)

# Load CSV to dataframe and then copy to table using PyMapD
def load_new_table_mapd(table_name, csv_file, mapd_host, mapd_user):
  global connection
  table_name = table_name.replace('.', '_')
  df = pd.read_csv(csv_file)
  df.reset_index(drop=True, inplace=True)
  print df.shape
  print df.head(10)
  drop_table_mapd(table_name)
  connection.create_table(table_name, df, preserve_index=False)
  connection.load_table(table_name, df) #, preserve_index=False)
  print connection.get_table_details(table_name)

# Copy CSV to MapD server and load table using COPY
def copy_and_load_table_mapd(table_name, csv_file, mapd_host, mapd_user):
  global connection
  table_name = table_name.replace('.', '_')
  create_table_str = 'CREATE TABLE IF NOT EXISTS %s (download_time TIMESTAMP, download_ip TEXT ENCODING DICT(8), download_package TEXT ENCODING DICT(8), ip_sequence FLOAT, cpu_gpu TEXT ENCODING DICT(8), package_type TEXT ENCODING DICT(8), target TEXT ENCODING DICT(8), arch TEXT ENCODING DICT(8), ip_lat FLOAT, ip_long FLOAT, ip_domain TEXT ENCODING DICT(8), ip_country TEXT ENCODING DICT(8))' % (table_name)
  print create_table_str
  connection.execute(create_table_str)
  server_csv_file = '/tmp/%s' % (os.path.basename(csv_file))
  command = 'scp %s %s@%s:%s' % (csv_file, mapd_user, mapd_host, server_csv_file)
  print command
  os.system(command)

  query = 'COPY %s from \'%s\' WITH (nulls = \'None\')' % (table_name, server_csv_file)
  print query
  connection.execute(query)
  print connection.get_table_details(table_name)

