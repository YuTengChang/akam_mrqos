#!//usr/bin/python
"""
Created on Thu March 09 12:47:15 2016

@author: ychang
"""
import sys, os
#import shutil

sys.path.append('/home/testgrp/MRQOS/')
import time
import datetime
import calendar
import configurations.config as config
import configurations.hdfsutil as hdfsutil
import configurations.beeline as beeline

def main():
    """ get the date for the past day (yesterday). """
    timenow = int(time.time())
    datenow = str(datetime.date.today()-datetime.timedelta(1))
    datenow = datenow[0:4]+datenow[5:7]+datenow[8:10]

    print "###################"
    print "# Start processing the data back in " + datenow + " (yesterday)"
    print "# starting processing time is " + str(timenow)
    print "###################"

    ts = calendar.timegm(time.gmtime())
    ts_last_hour = ts-3600
    datestamp = time.strftime('%Y%m%d', time.gmtime(float(ts_last_hour)))
    hourstamp = time.strftime('%H', time.gmtime(float(ts_last_hour)))

    # check if the summary has been performed on this particular hour (last hour)
    print "    ****  checking day = %s." % (datestamp),
    if hdfsutil.test_file(os.path.join(config.hdfs_qos_rg_day % (datestamp), '000000_0.deflate')):
        f = open(os.path.join(config.mrqos_hive_query, 'mrqos_region_summarize_day.hive'), 'r')
        strcmd = f.read()
        strcmd_s = strcmd % (datestamp, datestamp, datestamp)
        f.close()
        print "    ****  perform beeline for hourly summary for day = %s, hour = %s." %(datestamp, hourstamp)
        try:
            beeline.bln_e(strcmd_s)
        except:
            # delete the folder if summarization failed.
            print "    ****  summarization failed, removed hdfs folder."
            hdfsutil.rm(config.hdfs_qos_rg_day % (datestamp), r=True)
    else:
        print " file exists."







# ==============================================================================
# # remove partitions from hive table
# ==============================================================================
#def mrqos_region_cleanup():
#    """ when called, this function will delete all partitions
#        the mrqos_region table as long as it is older than the threshold """



if __name__ == '__main__':
    sys.exit(main())