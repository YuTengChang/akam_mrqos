#!/opt/anaconda/bin/python2.7
"""
Created on Thu Apr 16 16:47:15 2015

@author: ychang
"""
import sys, os
import shutil
import subprocess as sp
import time


def main():
    """
    getting the content a_maprule_qos_view_by_region table from mapmon and clean up (10 rows per case) and then upload
    to the cluster
    :return:
    """

    # variable settings
    query_retry_time = 1
    max_retrial = 20
    mapmon_machine = "72.246.193.143"
    mapmon_file = "/home/testgrp/full-table-mrqos-view-by-region"
    local_dir = "/home/ychang/Documents/Projects/18-DDC/MRQOS_local_data"
    mapmon_command = """ /a/bin/sql2 --csv ' select * from _local_a_maprule_qos_view_by_region ' > %s """ % mapmon_file
    scp_from_mapmon = """ scp -Sgwsh testgrp@%s:%s %s""" % (mapmon_machine, mapmon_file, os.path.join(local_dir, 'temp.csv'))

    # current time
    timenow = int(time.time())

    print "###################"
    print "Obtaining the a_maprule_qos_view_by_region content"
    print "starting processing time is " + str(timenow)
    print "###################"

    print "    ****  obtaining from mapmon."
    cmd_str = """ gwsh -2 %s "%s" """ % ( mapmon_machine, mapmon_command )
    sp.check_call(cmd_str, shell=True)
    print "    ****  scp file to local"
    sp.check_call(scp_from_mapmon, shell=True)

    # if query failed, re-try:
    while os.stat(os.path.join(local_dir, 'temp.csv')).st_size < 10000:
        print "    ****  obtaining from mapmon."
        cmd_str = """ gwsh -2 %s "%s" """ % ( mapmon_machine, mapmon_command )
        sp.check_call(cmd_str, shell=True)
        print "    ****  scp file to local"
        sp.check_call(scp_from_mapmon, shell=True)
        query_retry_time += 1
        if query_retry_time > max_retrial:
            print "    **** reach max re-trial, quitting..."
            return

    #

if __name__ == '__main__':
    sys.exit(main())
