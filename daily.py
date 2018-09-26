#!/usr/bin/python

import glob
import pandas as pd
import numpy as np
import os

Location = '/home/centos/daily_download/'

# Cleanup
commandline = "rm -f " +Location +"data/alldownloads.csv"
os.system(commandline)

# Import all of the csvs and concat them into a single data frame
allcsvs = Location + 'data/*.csv'
alldownloads = pd.concat([pd.read_csv(f, error_bad_lines=False, warn_bad_lines=False, header=None)
                          for f in glob.glob(allcsvs)], ignore_index = True)
print alldownloads[0].count()

# Name the columns from the log files
alldownloads.columns = ['download_time', 'download_ip', 'download_package']

# Add the new columns
alldownloads['ip_sequence'] = np.nan
alldownloads['cpu_gpu'] = np.nan
alldownloads['package_type'] = np.nan
alldownloads['target'] = np.nan
alldownloads['arch'] = np.nan
alldownloads['ip_lat'] = np.nan
alldownloads['ip_long'] = np.nan
alldownloads['ip_domain'] = np.nan
alldownloads['ip_country'] = np.nan

print alldownloads.head(5)

# Add the lat, long, country and domain based on the IP address
import geoip2.database
import sys
from socket import gethostbyaddr

# Get the readers for LatLong/Country and Domain DBs.
latlongReader = geoip2.database.Reader(Location + 'GeoLite2-City.mmdb')
domainReader = geoip2.database.Reader(Location + 'GeoIP2-Domain.mmdb')

# This is the dictionary we'll use to collect the sequences of IP addresses
seqIPs = { }
for index, download in alldownloads.iloc[0:].iterrows():
    sys.stdout.write('TRYING '+str(index)+'  '+download['download_ip']+'\n')
    if not download['download_ip'].startswith('10.'):
        try:
            latlong = latlongReader.city(download['download_ip'])
            alldownloads.loc[index, 'ip_lat'] = latlong.location.latitude
            alldownloads.loc[index, 'ip_long'] = latlong.location.longitude
            
            city = latlongReader.city(download['download_ip'])
            alldownloads.loc[index, 'ip_country'] = city.country.iso_code
        except:
            # Must be a bad IP address, drop it from the table
            # sys.stdout.write('BAD IP: '+download['IP Address']+'\n')
            alldownloads.drop(index, inplace=True)
            
        try:
            domain = domainReader.domain(download['download_ip'])
            alldownloads.loc[index, 'ip_domain'] = domain.domain
            # sys.stdout.write('YEP MAXMIND DOMAIN: '+domain.domain+'\n')
        except:
            sys.stdout.write('NO MAXMIND DOMAIN: '+download['download_ip']+'\n')
            # If MaxMind doesn't have it, try socket.gethostbyaddr()
            try:
                socketdomain = gethostbyaddr(str(download['download_ip']))
                splitdomain = socketdomain[0].split('.')
                # sys.stdout.write('  SOCKET DOMAIN: '+str(splitdomain)+'  '+str(len(splitdomain))+'\n')
                if not splitdomain[len(splitdomain)-1].isdigit():
                    domainrebuilt = splitdomain[len(splitdomain)-2] + '.' + splitdomain[len(splitdomain)-1]
                    if len(splitdomain[len(splitdomain)-1]) <= 2:
                        domainrebuilt = splitdomain[len(splitdomain)-3] + '.' + domainrebuilt
                    alldownloads.loc[index, 'ip_domain'] = domainrebuilt
                    # sys.stdout.write('  YES SOCKET DOMAIN: '+domainrebuilt+'\n')
                else:
                    # sys.stdout.write('  NUMBER SOCKET DOMAIN: \n')
                    pass
            except:
                # sys.stdout.write('  NOPE SOCKET DOMAIN EITHER \n')
                pass

    else:
        # Internal IP address, drop it from the table
        alldownloads.drop(index, inplace=True)
        continue

    # Add the sequence for this IP
    if download['download_ip'] in seqIPs:
        seqIPs[download['download_ip']] = seqIPs[download['download_ip']] + 1
        # sys.stdout.write('  IN THE SEQ '+str(seqIPs[download['download_ip']])+'\n')
    else:
        seqIPs[download['download_ip']] = 1
    alldownloads.loc[index, 'ip_sequence'] = seqIPs[download['download_ip']]
    
    # Add the CPU / GPU data
    if 'cpu' in download['download_package']:
        alldownloads.loc[index, 'cpu_gpu'] = 'CPU'
    else:
        alldownloads.loc[index, 'cpu_gpu'] = 'GPU'
    # sys.stdout.write('CPU '+str(download['cpu_gpu'])+'  '+download['download_package']+'\n')

    # Add the package type data
    if 'tar.gz' in download['download_package']:
        alldownloads.loc[index, 'package_type'] = 'tar'
    elif 'deb' in download['download_package']:
        alldownloads.loc[index, 'package_type'] = 'deb'
    else:
        alldownloads.loc[index, 'package_type'] = 'rpm'
    
    # Add the target data
    if '/aws' in download['download_package']:
        alldownloads.loc[index, 'target'] = 'AWS'
    elif '/gcp' in download['download_package']:
        alldownloads.loc[index, 'target'] = 'GCP'
    else:
        alldownloads.loc[index, 'target'] = 'Download'    

    # Add the architecture data
    if 'ppc' in download['download_package']:
        alldownloads.loc[index, 'arch'] = 'Power'
    else:
        alldownloads.loc[index, 'arch'] = 'x86'    
    
print alldownloads.head(5)
resultsFile = Location + "data/alldownloads.csv"
#alldownloads.to_csv(resultsFile, index_col = False)
alldownloads.to_csv(resultsFile)

# Using Boto3 library copy the final CSV to S3 bucket.
from boto3.session import Session

session = Session(profile_name='default')
s3 = session.resource('s3')
your_bucket = s3.Bucket('mapd-veda')

for s3_file in your_bucket.objects.all():
    print(s3_file.key)

s3.Object('mapd-veda', 'alldownloads.csv').delete()
s3.Object('mapd-veda', 'alldownloads.csv').put(Body=open(resultsFile, 'rb'))
object_acl = s3.ObjectAcl('mapd-veda', 'alldownloads.csv')
response = object_acl.put(ACL='public-read')

# Copy the final CSV file from S3 to MapD table.
# Assuming the table was already created and so we are appending data.
mapd_utils_with_path = Location + 'mapd_utils'
sys.path.append(Location)
from mapd_utils import *

# Connect to MapD
connect_to_mapd("ACCESS_KEY", "SECRET_KEY", "use2-api.mapd.cloud", "mapd")

# Load data into MapD table
load_table_mapd("vs_mapd_downloads_seq", "mapd-veda/alldownloads.csv", "localhost", "centos")

# Disconnect MapD
disconnect_mapd()

