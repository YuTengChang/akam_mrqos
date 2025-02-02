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

def main():
    """  this function will compute the statistics of MRQOS tables within the
    past two weeks (X-1 day : X-15 days) """
    timenow = int(time.time())
    datenow = str(datetime.date.today()-datetime.timedelta(1))
    datestamp = time.strftime('%Y%m%d', time.gmtime(float(timenow-86400)))
    datenow = datenow[0:4]+datenow[5:7]+datenow[8:10]
    print "###################"
    print "# Start processing the data back in " + datenow + " for two-week window"
    print "# starting processing time is " + str(timenow)
    print "###################"
    max_retrial = 10

    # update the ts table for later summarize usage. file uploaded to HDFS
    print "    ****  create new mrqos_ts table."
    sp.call([config.create_ts_table], shell=True)

    # summarize script old version
    # open the file for writing the results
    print "    ****  running hive summarizing script."
    retrial = 0
    while retrial < max_retrial:
        try:
            tic = time.time()
            f = open('/home/testgrp/MRQOS/mrqos_data/summarized_table.tmp','w')
            sp.check_call(['hive','-f','/home/testgrp/MRQOS/MRQOS_table_summarize.hive'],stdout=f)
            print "    # success with time cost = %s" % str(time.time()-tic)
            break
        except:
            retrial += 1
            print "    # failed retrial #%s with time cost = %s" % (str(retrial), str(time.time()-tic))
        f.close()


    # process the file, take country only
    cmd = """cat /home/testgrp/MRQOS/mrqos_data/summarized_table.tmp | sed s:NULL:0:g | sed 's/\t/,/g' | awk -F',' '{x=length($4); if(x==2){print $0;}}' | awk -F',' '{if($3>0){$1=""; $2=""; print $0;}}' | sed 's/^\s\+//g' > /home/testgrp/MRQOS/mrqos_data/summarized_processed.tmp""";
    sp.check_call( cmd, shell=True )

    # upload the summarized table in hive
    print "    ****  upload the summarized table to HDFS."
    listname = os.path.join(config.mrqos_data, 'summarized_processed.tmp')
    hdfs_d = os.path.join(config.hdfs_table,'mrqos_sum','ts=%s' % str(datenow))
    upload_to_hive(listname, hdfs_d, str(datenow), 'mrqos_sum')


    # summarize script new version
    # open the file for writing the results
    print "    ****  running hive summarizing script (new)."
    retrial = 0
    while retrial < max_retrial:
        try:
            tic = time.time()
            f = open('/home/testgrp/MRQOS/mrqos_data/summarized_table2.tmp','w')
            sp.check_call(['hive','-f','/home/testgrp/MRQOS/mrqos_hive_query/MRQOS_table_summarize2.hive'],stdout=f)
            print "    # success with time cost = %s" % str(time.time()-tic)
            break
        except:
            retrial += 1
            print "    # failed retrial #%s with time cost = %s" % (str(retrial), str(time.time()-tic))
        f.close()


    # process the file, take country only
    cmd = """cat /home/testgrp/MRQOS/mrqos_data/summarized_table2.tmp | sed s:NULL:0:g | sed 's/\t/,/g' | awk -F',' '{x=length($4); if(x==2){print $0;}}' | awk -F',' '{if($3>0){$1=""; $2=""; print $0;}}' | sed 's/^\s\+//g' > /home/testgrp/MRQOS/mrqos_data/summarized_processed2.tmp""";
    sp.check_call( cmd, shell=True )

    # upload the summarized table in hive
    print "    ****  upload the summarized table to HDFS."
    listname = os.path.join(config.mrqos_data, 'summarized_processed2.tmp')
    hdfs_d = os.path.join(config.hdfs_table,'mrqos_sum2','ts=%s' % str(datenow))
    upload_to_hive(listname, hdfs_d, str(datenow), 'mrqos_sum2')


    # compute COMPOUND-ERROR-METRIC
    print "    ****  running hive script for compound error metrics."
    retrial = 0
    while retrial < max_retrial:
        try:
            tic = time.time()
            f = open(os.path.join(config.mrqos_hive_query, 'MRQOS_table_levels.hive'), 'r')
            strcmd = f.read()
            strcmd_s = strcmd % (datestamp, datestamp, datestamp, datestamp, datestamp,
                                 datestamp, datestamp, datestamp, datestamp, datestamp,
                                 datestamp)
            f.close()

            f = open('/home/testgrp/MRQOS/mrqos_data/compound_metric_full.tmp','w')
            beeline.bln_e_output(strcmd_s, '/home/testgrp/MRQOS/mrqos_data/compound_metric_full.tmp')
            print "    # success with time cost = %s" % str(time.time()-tic)
            # replaced code:
            #sp.check_call(['hive','-f','/home/testgrp/MRQOS/MRQOS_table_levels.hive'],stdout=f)
            f1 = '/home/testgrp/MRQOS/mrqos_data/compound_metric_full.tmp'
            f2 = '/home/testgrp/MRQOS/mrqos_data/compound_metric.tmp'
            cmd_str = ''' cat %s | awk '{print $1, $2, $3, $4, $5, $6, $14, $15, $16, $17, $18, $26, $27, $28, $29, $30, $38, $39, $40, $41, $42, $50, $51}' > %s ''' % (f1, f2)
            sp.check_call(cmd_str, shell=True)
            break
        except:
            retrial += 1
            print "    # failed retrial #%s with time cost = %s" % (str(retrial), str(time.time()-tic))
    f.close()

    # obtain the summarized statistics that spanned [-28d, -14d]
    print "    ****  running hive queries for 2w comparisons."
    cmdstr = '''/usr/bin/python /home/testgrp/MRQOS/mrqos_python_script/mrqos_sum_comparison.py'''
    sp.check_call(cmdstr, shell=True)


def upload_to_hive(listname, hdfs_d, ts, tablename):
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
        hiveql_str = 'use mrqos; alter table ' + tablename + ' add partition(ts=%s);' % (ts)
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
    sys.exit(main())
