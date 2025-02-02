#!/opt/anaconda/bin/python2.7
"""
Created on Thu Apr 16 16:47:15 2015

@author: ychang
"""
import sys
import os
#import shutil
import subprocess as sp
import time
sys.path.append('/home/ychang/Documents/Projects/18-DDC/MRQOS/')
import configurations.config as config



def main():
    """
    getting the content a_maprule_qos_view_by_region table from mapmon and clean up (10 rows per case) and then upload
    to the cluster
    :return:
    """

    # variable settings
    query_retry_time = 1
    max_retrial = 20
    mapmon_machine = sp.check_output('/home/ychang/bin/bin/mapper-leader mapmon', shell=True)
    mapmon_machine = mapmon_machine.strip()
    cluster_namenode = "81.52.137.180"
    mapmon_file = "/home/testgrp/full-table-mrqos-view-by-region"
    local_dir = "/home/ychang/Documents/Projects/18-DDC/MRQOS_local_data"
    # already retry 20 times on mapmon machine
    mapmon_query_bash_file = "/home/ychang/Documents/Projects/18-DDC/MRQOS/mrqos_shell_scripts/mapmon_query_view_by_region.sh"
    scp_from_mapmon = """ scp -Sgwsh testgrp@%s:%s %s""" % (mapmon_machine, mapmon_file, os.path.join(local_dir, 'temp.csv'))
    cleanup_command_1 = """ cat %s | tail -n+3 | sort -t"," -k9gr | awk -F, '{id=$1; count[id]+=1; cum_pert[id]+=$NF; if(count[id]<=30){split($1,a,"."); split(a[2],mr,"_"); split(a[3],geo,"_"); if(length($3)==0){net="ANY";}else{net=$3;} if(length(geo[2])==2){print $1, mr[2], geo[2], net, $5, $6, $7, $8, $9, $NF, cum_pert[id];}}}' > %s""" % (os.path.join(local_dir,'temp.csv'),
                                                                                                                                                                                                                                                                                                   os.path.join(local_dir,'temp1.csv') )
    cleanup_command_2 = """ awk 'NR==FNR{id=$1; count[id]+=$9; if(cum[id]<$11){cum[id]=$11;} next}{id=$1; if(count[id]>0){load_ratio=100*$9/count[id];}else{load_ratio=-1;} print $2,$3,$4,$5,$6,$7,$8,$9,$10, load_ratio, cum[id];}' %s %s > %s""" %( os.path.join(local_dir,'temp1.csv'),
                                                                                                                                                                                                        os.path.join(local_dir,'temp1.csv'),
                                                                                                                                                                                                        os.path.join(local_dir,'temp2.csv'))

    # current time
    timenow = int(time.time())

    print "###################"
    print "# Obtaining the a_maprule_qos_view_by_region content"
    print "# starting processing time is " + str(timenow)
    print "###################"

    print "    ****  mapmon leader machine: " + mapmon_machine

    # try the mapmon bash processing (db acquire)
    try:
        print "    ****  obtaining database at mapmon."
        cmd_str = """ gwsh -2 %s "bash -s" <  %s """ % ( mapmon_machine, mapmon_query_bash_file )
        print "    ****  command: " + cmd_str
        sp.check_call(cmd_str, shell=True)
    except:
        print "    **** # bash process at mapmon failure. "
        cmd_str = """ echo 'dummy' > %s """ % os.path.join(local_dir, 'temp.csv')
        sp.check_call(cmd_str, shell=True)

    # try copy the db to local
    try:
        print "    ****  scp file to local"
        print "    ****  scp command: " + scp_from_mapmon
        sp.check_call(scp_from_mapmon, shell=True)
    except:
        print "    **** # scp failure. "
        cmd_str = """ echo 'dummy' > %s """ % os.path.join(local_dir, 'temp.csv')
        sp.check_call(cmd_str, shell=True)

    # if query failed, re-try:
    while os.stat(os.path.join(local_dir, 'temp.csv')).st_size < 10000:
        mapmon_machine = sp.check_output('/u4/ychang/bin/mapper-leader mapmon', shell=True)
        mapmon_machine = mapmon_machine.strip()
        scp_from_mapmon = """ scp -Sgwsh testgrp@%s:%s %s""" % (mapmon_machine, mapmon_file, os.path.join(local_dir, 'temp.csv'))
        print "    ****  obtaining database at mapmon."
        cmd_str = """ gwsh -2 %s "bash -s" < %s """ % ( mapmon_machine, mapmon_query_bash_file )
        try:
            sp.check_call(cmd_str, shell=True)
        except:
            print "    **** # bash process at mapmon failure. "
            cmd_str = """ echo 'dummy' > %s """ % os.path.join(local_dir, 'temp.csv')
            sp.check_call(cmd_str, shell=True)

        try:
            print "    ****  scp file to local"
            sp.check_call(scp_from_mapmon, shell=True)
        except:
            print "    **** # scp failure. "
            cmd_str = """ echo 'dummy' > %s """ % os.path.join(local_dir, 'temp.csv')
            sp.check_call(cmd_str, shell=True)

        # ending mechanism
        query_retry_time += 1
        if query_retry_time > max_retrial:
            print "    **** reach max re-trial, quitting..."
            return

    # clean-up the temp local file
    print "    ****  clean up the file part 1."
    sp.check_call(cleanup_command_1, shell=True)
    print "    ****  clean up the file part 2."
    sp.check_call(cleanup_command_2, shell=True)

    # upload to the cluster
    print "    ****  upload to the cluster."
    cmd_str = """ scp -Sgwsh %s testgrp@%s:%s/qos_region.%s.tmp """ % (os.path.join(local_dir, 'temp2.csv'),
                                                                       cluster_namenode,
                                                                       config.mrqos_data,
                                                                       str(timenow))
    print "    **** command: %s" % (cmd_str)
    sp.check_call(cmd_str, shell=True)

if __name__ == '__main__':
    sys.exit(main())
