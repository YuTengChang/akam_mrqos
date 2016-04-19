#!//usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 28 11:31:55 2015

@author: ychang
"""

import sys,os
sys.path.append('/home/testgrp/MRQOS/')
import subprocess as sp
import time
import datetime
import configurations.config as config
import configurations.hdfsutil as hdfsutil
import configurations.beeline as beeline
import getopt

def main(argv):
    """  this function will compute the statistics of MRQOS tables within the
    past two weeks (X-1 day : X-2 days) and (X-1 day : X-4 days)"""
    try:
        opts, args = getopt.getopt(argv, "hd:", ["help", "datestamp="])
    except getopt.GetoptError:
        print 'mrqos_summarize_at_X_1d3d.py -d <datestamp>'
    #    sys.exit(2)

    datestamp = ''

    for opt, arg in opts:
        if opt == '-h':
            print 'mrqos_summarize_at_X_1d3d.py -d <datestamp>'
            sys.exit()
        elif opt in ("-d", "--datestamp"):
            datestamp = arg

    timenow = int(time.time())
    datenow = str(datetime.date.today()-datetime.timedelta(1))
    datenow = datenow[0:4]+datenow[5:7]+datenow[8:10]
    print "###################"
    print "# Start processing the data back in " + datenow + " for 1d/3d window"
    print "# starting processing time is " + str(timenow)
    print "###################"
    max_retrial = 10

    # this is one day summary (last day, partition datestamp = X-1)
    print "    #****  running hive summarizing script."
    script_file = os.path.join(config.mrqos_hive_query, 'MRQOS_table_summarize_at_X_1d.hive')
    epochtime = str(int( time.mktime(time.strptime(datestamp,'%Y%m%d')) ))

    f = open(script_file, 'r')
    strcmd = f.read()
    strcmd_s = strcmd % (epochtime, epochtime, epochtime, epochtime)
    f.close()



    output_file = os.path.join(config.mrqos_data, 'summarized_1d_at_X.tmp')
    processed_file = os.path.join(config.mrqos_data, 'summarized_processed_1d.tmp')
    my_retrial(max_retrial, strcmd_s, output_file=output_file)
    # process the file, take country only
    cmd = """cat %s | sed s:NULL:0:g | sed 's/\t/,/g' | awk -F',' '{x=length($4); if(x==2){print $0;}}' | awk -F',' '{if($3>0){$1=""; $2=""; print $0;}}' | sed 's/^\s\+//g' > %s""" % (output_file,
                                                                                                                                                                                        processed_file)
    sp.check_call(cmd, shell=True)

    # upload the summarized table in hive
    #print "    #****  upload the summarized table to HDFS."
    #hdfs_d = os.path.join(config.hdfs_table,'mrqos_sum_1d','datestamp=%s' % str(datenow))
    #upload_to_hive(processed_file, hdfs_d, 'datestamp', str(datenow), 'mrqos_sum_1d')


    # this is three-day summary (last 3 days, partition datestamp = X-1)
    #print "    #****  running hive summarizing script."
    #script_file = '/home/testgrp/MRQOS/mrqos_hive_query/MRQOS_table_summarize_3d.hive'
    #output_file = os.path.join(config.mrqos_data, 'summarized_3d.tmp')
    #processed_file = os.path.join(config.mrqos_data, 'summarized_processed_3d.tmp')
    #my_retrial(max_retrial, script_file, output_file=output_file)
    # process the file, take country only
    #cmd = """cat %s | sed s:NULL:0:g | sed 's/\t/,/g' | awk -F',' '{x=length($4); if(x==2){print $0;}}' | awk -F',' '{if($3>0){$1=""; $2=""; print $0;}}' | sed 's/^\s\+//g' > %s""" % (output_file,
    #                                                                                                                                                                                    processed_file)
    #sp.check_call(cmd, shell=True)

    # upload the summarized table in hive
    #print "    #****  upload the summarized table to HDFS."
    #hdfs_d = os.path.join(config.hdfs_table,'mrqos_sum_3d','datestamp=%s' % str(datenow))
    #upload_to_hive(processed_file, hdfs_d, 'datestamp', str(datenow), 'mrqos_sum_3d')

    return


def my_retrial(max_retrial, strcmd_s, output_file=''):
    retrial = 0
    while retrial < max_retrial:
        try:
            tic = time.time()
            if len(output_file) > 0:
                f = open('%s' % output_file, 'w')
                #print "  file opened: %s" % output_file
                sp.check_call(['hive', '-e', '%s' % strcmd_s], stdout=f)
                #print "  script running: %s" % strcmd_s
                f.close()
            else:
                sp.check_call(['hive', '-e', '%s' % strcmd_s])
            print "    # success with time cost = %s" % str(time.time()-tic)

            break
        except:
            retrial += 1
            print "    # failed retrial #%s with time cost = %s" % (str(retrial), str(time.time()-tic))


def upload_to_hive(listname, hdfs_d, partition_name, partition_idx, tablename):
    """ this function will create a partition directory in hdfs with the requisite timestamp. It will
    then add the partition to the table cl_ns_pp with the appropriate timestamp """

    #hdfs_d = config.hdfsclnspp % (ts)
    # create the partition
    try:
        sp.check_call(['hadoop', 'fs', '-mkdir', hdfs_d])
    # upload the data
    except sp.CalledProcessError:
        raise HadoopDirectoryCreateError
    try:
        sp.check_call(['hadoop', 'fs', '-put', listname, hdfs_d])
    except sp.CalledProcessError:
        raise HadoopDataUploadError

    # add the partition
    try:
        hiveql_str = 'use mrqos; alter table %s add partition(%s=%s);' % (tablename,
                                                                          partition_name,
                                                                          partition_idx)
        sp.check_call(['hive', '-e', hiveql_str])
    except sp.CalledProcessError:
        raise HiveCreatePartitionError

#==============================================================================
# # hdfs error category
#==============================================================================
class HadoopDirectoryCreateError(Exception):
    def __init__(self):
        self.message = "Unable to create directory."

class HadoopDataUploadError(Exception):
    def __init__(self):
        self.message = "Unable to upload data to hdfs."

class HiveCreatePartitionError(Exception):
    def __init__(self):
        self.message = "Unable to create partition"

class GenericHadoopError(Exception):
    def __init__(self):
        self.message = "Something went wrong in deleting a partition or associated data"

if __name__=='__main__':
    sys.exit(main(sys.argv[1:]))