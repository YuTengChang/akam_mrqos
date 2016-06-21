#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 31 15:58:55 2015

@author: ychang

This script do the insertion of NS-Info files

"""
import sys,os
sys.path.append('/home/testgrp/MRQOS/')
import glob
import configurations.hdfsutil as hdfs
import configurations.config as config
import configurations.beeline as beeline
import time
import subprocess as sp


def main():
    # #### MRQOS region LOCAL PART ####
    # ignore the local timestamp, use what files are tagged
    timenow = time.time()
    print "###################"
    print "# Performing the hourly mrqos_region insert"
    print "# starting processing time is " + str(timenow)
    print "###################"

    list_qos_files = glob.glob( os.path.join(config.mrqos_data,
                                            'qos_region.*.tmp') ) # glob get the full path
    for qos_file in list_qos_files:
        infoitem = qos_file.rsplit('.',2)
        ts = infoitem[-2]
        datestamp = time.strftime('%Y%m%d', time.localtime(float(ts)))
        # do we need hourly partition or not?
        hourstamp = time.strftime('%H', time.localtime(float(ts)))

        print '    file = ' + qos_file
        print '    timestamp = %s;' % ( ts )

        # put the file to HDFS folder and remove from Local
        try:
            print '    upload to HDFS'
            hdfs_rg_destination = config.hdfs_qos_rg_info % ( datestamp, hourstamp, ts )
            hdfs.mkdir( hdfs_rg_destination )
            hdfs.put( qos_file, hdfs_rg_destination )

            print '    adding partition'
            hiveql_str = config.add_rg_partition % ( datestamp, hourstamp, ts )
            beeline.bln_e(hiveql_str)

            print '    remove local file: ' + qos_file
            os.remove(qos_file)
        except sp.CalledProcessError as e:
            print e.message
            print 'MRQOS region(RG) information update failed for timestamp=%s' % ( ts )
            if 'File exists' in e.message:
                print '    remove local file: ' + qos_file
                os.remove(qos_file)



if __name__=='__main__':
    sys.exit(main())
