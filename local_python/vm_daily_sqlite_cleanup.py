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
sys.path.append('/home/ychang/Documents/Projects/18-DDC/MRQOS/')
import configurations.config as config

def main():
    ts = calendar.timegm(time.gmtime())
    region_view_hour_db = '/opt/web-data/SQLite3/ra_mrqos.db'
    case_view_hour_db = '/opt/web-data/SQLite3/case_view_hour.db'


    print "now do the cleaning on VM: region_view_hour"
    expire_region_view_hour_vm = 60*60*24*(4+1) # 1+4 days expiration (~ 1-week)
    expire_date = time.strftime('%Y%m%d', time.gmtime(float(ts - expire_region_view_hour_vm)))
    sql_str = '''PRAGMA temp_store_directory='/opt/web-data/temp'; delete from region_view_hour where date=%s; vacuum;''' % str(expire_date)
    cmd_str = '''/opt/anaconda/bin/sqlite3 %s "%s"  ''' % (region_view_hour_db,
                                                           sql_str)
    # print cmd_str
    sp.check_call(cmd_str, shell=True)

    print "now do the cleaning on VM: case_view_hour"
    expire_region_view_hour_vm = 60*60*24*(4+1) # 1+4 days expiration (~ 1-week)
    expire_date = time.strftime('%Y%m%d', time.gmtime(float(ts - expire_region_view_hour_vm)))
    sql_str = '''PRAGMA temp_store_directory='/opt/web-data/temp'; delete from case_view_hour where date=%s; vacuum;''' % str(expire_date)
    cmd_str = '''/opt/anaconda/bin/sqlite3 %s "%s"  ''' % (case_view_hour_db,
                                                           sql_str)
    # print cmd_str
    sp.check_call(cmd_str, shell=True)

if __name__ == '__main__':
    sys.exit(main())
