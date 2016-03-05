#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 31 15:58:55 2015

@author: ychang

This script do the insertion of NS-Info files

"""
import sys,os
sys.path.append('/home/testgrp/MRQOS/')
import subprocess as sp
import glob
import configurations.hdfsutil as hdfs
import configurations.config as config
import configurations.beeline as beeline


def main():
    # #### MRQOS region LOCAL PART ####
    # ignore the local timestamp, use what files are tagged
    list_qos_files = glob.glob( os.path.join(config.mrqos_data,
                                            'qos_region.*.txt') ) # glob get the full path
    for qos_file in list_qos_files:
        infoitem = qos_file.rsplit('_',2)
        ts = infoitem[1]
        print '    file = ' + qos_file
        print '    timestamp = %s; UUID = %s' % ( ts )

        # put the file to HDFS folder and remove from Local
        try:
            print '    upload to HDFS'
            hdfs_ns_destination = config.hdfs_qos_rg_info % ( ts )
            hdfs.mkdir( hdfs_ns_destination )
            hdfs.put( qos_file, hdfs_ns_destination )

            print '    adding partition'
            hiveql_str = config.add_ns_partition % ( datestamp, UUID )
            print '    '+hiveql_str
            sp.check_call(['hive','-e',hiveql_str])
            print '    remove local file: ' + qos_file
            os.remove(qos_file)
        except:
            print 'resolver(NS) information update failed for timestamp=%s' % ( ts )


if __name__=='__main__':
    sys.exit(main())
