#!//usr/bin/python
"""
Created on Thu March 09 12:47:15 2016

@author: ychang
    retired code for mrqos_region_view_hour.hive
"""
import sys, os
#import shutil

sys.path.append('/home/testgrp/MRQOS/')
import time
import calendar
import configurations.config as config
import configurations.hdfsutil as hdfsutil
import configurations.beeline as beeline

def main():
    """ get the date and hour for the previous hour. Will check from the beginning of the day, insert when missing. """
    ts = calendar.timegm(time.gmtime())
    print "###################"
    print "# Performing the hourly mrqos_region summary"
    print "# starting processing time is " + str(ts)
    print "###################"
    ts_last_hour = ts-3600
    datestamp = time.strftime('%Y%m%d', time.gmtime(float(ts_last_hour)))
    hourstamp = time.strftime('%H', time.gmtime(float(ts_last_hour)))
    hour_list = [str("%02d" % x) for x in range(24)]
    region_summary_retrial_max = 10

    # check if the summary has been performed on this particular hour (last hour)
    print "    ****  checking day = %s, hour = %s." % (datestamp, hourstamp),
    if hdfsutil.test_file(os.path.join(config.hdfs_qos_rg_view_hour % (datestamp, hourstamp), '000000_0.deflate')):
        f = open(os.path.join(config.mrqos_hive_query, 'mrqos_region_view_hour.hive'), 'r')
        strcmd = f.read()
        strcmd_s = strcmd % (datestamp, hourstamp, datestamp, hourstamp, datestamp, hourstamp)
        f.close()
        strcmd_g = "select * from mrqos.region_view_hour where datestamp=%s and hour=%s;" % (datestamp, hourstamp)
        query_result_file = os.path.join(config.mrqos_query_result,'region_view_hour.%s.%s.csv' % (datestamp, hourstamp))
        print "    ****  perform beeline for hourly summary for day = %s, hour = %s." % (datestamp, hourstamp)
        count_retrial = 0
        while count_retrial < region_summary_retrial_max:
            try:
                beeline.bln_e(strcmd_s)
                try:
                    beeline.bln_e_output(strcmd_g, query_result_file)
                except:
                    print "    ****  copy to local failed!"
                break
            except:
                # delete the folder if summarization failed.
                print "    ****  summarization failed upto #retrials="+str(count_retrial)
                hdfsutil.rm(config.hdfs_qos_rg_view_hour % (datestamp, hourstamp), r=True)
                count_retrial += 1

    else:
        print " file exists."

    # check if the summary has been performed since the beginning of the day, last check on day X is X+1/0:30:00
    for hour in hour_list:
        if hour < hourstamp:
            print "    ****  checking day = %s, hour = %s." % (datestamp, hour),
            if hdfsutil.test_file(os.path.join(config.hdfs_qos_rg_view_hour % (datestamp, hour), '000000_0.deflate')):
                f = open(os.path.join(config.mrqos_hive_query, 'mrqos_region_view_hour.hive'), 'r')
                strcmd = f.read()
                strcmd_s = strcmd % (datestamp, hour, datestamp, hour, datestamp, hour)
                f.close()
                print "    ****  perform beeline for hourly summary for day = %s, hour = %s." %(datestamp, hour)
                try:
                    beeline.bln_e(strcmd_s)
                except:
                    # delete the folder if summarization failed.
                    print "    ****  summarization failed, removed hdfs folder."
                    hdfsutil.rm(config.hdfs_qos_rg_view_hour % (datestamp, hour), r=True)
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