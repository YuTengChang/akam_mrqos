#!/opt/anaconda/bin/python
"""
Created on Thu March 09 12:47:15 2016

@author: ychang
"""
import sys, os
import shutil
import time
import calendar
import subprocess as sp

def main():
    ts = calendar.timegm(time.gmtime())
    print "###################"
    print "# Performing the hourly region_view_hour data fetch and insert"
    print "# starting processing time is " + str(ts)
    print "###################"
    ts_last_hour = ts-3600-1800
    datestamp = time.strftime('%Y%m%d', time.gmtime(float(ts_last_hour)))
    hourstamp = time.strftime('%H', time.gmtime(float(ts_last_hour)))
    filename = 'region_view_hour.%s.%s.csv' % (datestamp, hourstamp)
    local_data_repot = '/home/ychang/Documents/Projects/18-DDC/MRQOS_local_data/region_view_hour/'
    local_file = os.path.join(local_data_repot, filename)
    local_temp = os.path.join(local_data_repot, 'temp.csv')

    # fetch the data file from the cluster
    try:
        cmd_str = 'scp -Sgwsh testgrp@81.52.137.195:/home/testgrp/query_results/%s %s' % (filename, local_temp)
        sp.check_call(cmd_str, shell=True)
        # cluster file remove
        cmd_str = 'gwsh 81.52.137.195 "rm /home/testgrp/query_results/%s"' % filename
        sp.check_call(cmd_str, shell=True)
    except:
        print '    ****  data copy from cluster failure.'
        return

    # insert into the SQLite3 database
    # make file comma separated file (CSV)
    cmd_str = "cat %s | tail -n+2 | sed 's/\t/,/g' > %s" % (local_temp, local_file)
    sp.check_call(cmd_str, shell=True)
    # prepare the import sql
    cmd_str = "echo '.separator ,' > %s" % os.path.join(local_data_repot, 'input_query.sql')
    sp.check_call(cmd_str, shell=True)
    cmd_str = "echo '.import %s region_view_hour' >> %s" % (local_file, os.path.join(local_data_repot, 'input_query.sql'))
    sp.check_call(cmd_str, shell=True)
    # data import
    cmd_str = '/opt/anaconda/bin/sqlite3 /opt/web-data/SQLite3/ra_mrqos.db < %s' % os.path.join(local_data_repot, 'input_query.sql')
    sp.check_call(cmd_str, shell=True)

    # remove local file
    os.remove(local_temp)
    os.remove(local_file)

    # expire the data from SQLite database
    expire_region_view_hour = 60*60*24*5 # 5 days expiration
    expire_date = time.strftime('%Y%m%d', time.gmtime(float(ts - expire_region_view_hour)))
    sql_str = 'delete from region_view_hour where date=%s' % str(expire_date)
    cmd_str = '/opt/anaconda/bin/sqlite3 /opt/web-data/SQLite3/ra_mrqos.db "%s"' % sql_str
    sp.check_call(cmd_str, shell=True)



if __name__ == '__main__':
    sys.exit(main())
