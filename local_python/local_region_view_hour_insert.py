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
    print "###################"
    print "# Performing the hourly region_view_hour data fetch and insert"
    print "# starting processing time is " + str(ts) + " = " + time.strftime('GMT %Y-%m-%d %H:%M:%S', time.localtime(ts))
    print "###################"

    cmd_str = 'gwsh %s "ls %s/region_view_hour.*.csv"' % (config.region_view_hour_data_source, config.mrqos_query_result)
    try:
        remote_file_list = sp.check_output(cmd_str, shell=True).strip().split('\n')
    except sp.CalledProcessError as e:
        print "no remote file(s) or cannot connect to remote cluster, stopping."
        return


    for target_file in remote_file_list:
        print "processing the file: %s" % target_file
        target_file = target_file.split('/')[-1]
        # copy from cluster to local
        cmd_str = 'scp -Sgwsh testgrp@%s:%s/%s %s%s' % (config.region_view_hour_data_source,
                                                        config.mrqos_query_result,
                                                        target_file,
                                                        config.region_view_hour_data_local,
                                                        target_file+'.tmp')
        sp.check_call(cmd_str, shell=True)

        local_file = os.path.join(config.region_view_hour_data_local, target_file)
        local_temp = os.path.join(config.region_view_hour_data_local, target_file+'.tmp')

        # remove the file from the cluster
        cmd_str = 'gwsh %s "rm %s/%s"' % (config.region_view_hour_data_source,
                                          config.mrqos_query_result,
                                          target_file)
        sp.check_call(cmd_str, shell=True)

        # reformatting the file
        cmd_str = "cat %s | tail -n+2 | sed 's/\t/,/g' > %s" % (local_temp,
                                                                local_file)
        sp.check_call(cmd_str, shell=True)

        # prepare the import sql
        cmd_str = "echo '.separator ,' > %s" % os.path.join(config.region_view_hour_data_local, 'input_query.sql')
        sp.check_call(cmd_str, shell=True)
        cmd_str = "echo '.import %s region_view_hour' >> %s" % (local_file,
                                                                os.path.join(config.region_view_hour_data_local, 'input_query.sql'))
        sp.check_call(cmd_str, shell=True)
        # data import
        cmd_str = '/opt/anaconda/bin/sqlite3 %s < %s' % (config.region_view_hour_db,
                                                         os.path.join(config.region_view_hour_data_local, 'input_query.sql'))
        sp.check_call(cmd_str, shell=True)

        # remove local file
        # os.remove(local_temp) #< this could be a backup.
        os.remove(local_file)

    # expire the data from SQLite database
    print "now do the cleaning."
    expire_region_view_hour = config.region_view_hour_delete # 5 days expiration
    expire_date = time.strftime('%Y%m%d', time.gmtime(float(ts - expire_region_view_hour)))
    sql_str = 'delete from region_view_hour where date=%s' % str(expire_date)
    cmd_str = '/opt/anaconda/bin/sqlite3 %s "%s"' % (config.region_view_hour_db,
                                                     sql_str)
    sp.check_call(cmd_str, shell=True)


    # ################# #
    # CASE VIEW PROCESS #
    # ################# #

if __name__ == '__main__':
    sys.exit(main())
