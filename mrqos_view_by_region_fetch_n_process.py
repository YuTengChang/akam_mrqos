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
    max_retrial = 10
    mapmon_machine = sp.check_output('/u4/ychang/bin/mapper-leader mapmon', shell=True)
    mapmon_machine = mapmon_machine.strip()
    mapmon_file = "/home/testgrp/full-table-mrqos-view-by-region"
    local_dir = "/home/ychang/Documents/Projects/18-DDC/MRQOS_local_data"
    # already retry 20 times on mapmon machine
    mapmon_command = """ count=0; line=0; while(( "$count" < "20" )) && (( "$line" < "10" )); do /a/bin/sql2 --csv ' select * from a_maprule_qos_view_by_region ' >  %s; line=`wc -l %s | awk '{print $1;}'`; count=$((count+1)); done; """ % (mapmon_file, mapmon_file)
    scp_from_mapmon = """ scp -Sgwsh testgrp@%s:%s %s""" % (mapmon_machine, mapmon_file, os.path.join(local_dir, 'temp.csv'))
    cleanup_command_1 = """ cat %s | tail -n+3 | sort -t"," -k9gr | awk -F, '{id=$1; count[id]+=1; cum_pert[id]+=$NF; if(count[id]<10){split($1,a,"."); split(a[2],mr,"_"); split(a[3],geo,"_"); split(a[4],net,"_"); print $1, mr[2], geo[2], net[2], $5, $7, $8, $9, cum_pert[id];}}' > %s""" % (os.path.join(local_dir,'temp.csv'),
                                                                                                                                                                                                                                                                                                   os.path.join(local_dir,'temp1.csv'))

    # current time
    timenow = int(time.time())

    print "###################"
    print "Obtaining the a_maprule_qos_view_by_region content"
    print "starting processing time is " + str(timenow)
    print "###################"

    print "    ****  mapmon leader machine:" + mapmon_machine

    print "    ****  obtaining from mapmon."
    cmd_str = """ gwsh -2 %s "%s" """ % ( mapmon_machine, mapmon_command )
    print cmd_str
    sp.check_call(cmd_str, shell=True)
    print "    ****  scp file to local"
    sp.check_call(scp_from_mapmon, shell=True)

    # if query failed, re-try:
    while os.stat(os.path.join(local_dir, 'temp.csv')).st_size < 10000:
        mapmon_machine = sp.check_output('/u4/ychang/bin/mapper-leader mapmon', shell=True)
        mapmon_machine = mapmon_machine.strip()
        scp_from_mapmon = """ scp -Sgwsh testgrp@%s:%s %s""" % (mapmon_machine, mapmon_file, os.path.join(local_dir, 'temp.csv'))
        print "    ****  obtaining from mapmon."
        cmd_str = """ gwsh -2 %s "%s" """ % ( mapmon_machine, mapmon_command )
        sp.check_call(cmd_str, shell=True)
        print "    ****  scp file to local"
        sp.check_call(scp_from_mapmon, shell=True)
        query_retry_time += 1
        if query_retry_time > max_retrial:
            print "    **** reach max re-trial, quitting..."
            return

    # clean-up the temp local file
    print "    ****  clean up the file part 1."
    sp.check_call(cleanup_command_1, shell=True)

if __name__ == '__main__':
    sys.exit(main())
