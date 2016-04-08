#!//usr/bin/python
"""
Created on Thu March 09 12:47:15 2016

@author: ychang
"""
import sys, os
#import shutil

sys.path.append('/home/testgrp/MRQOS/')
import time
import calendar
import configurations.config as config
import configurations.hdfsutil as hdfsutil
import configurations.beeline as beeline
import subprocess as sp
import getopt

def main(argv):
    """ get the date and hour for the specified day and hour. Clean(drop) and rebuild the table partition. """
    try:
        opts, args = getopt.getopt(argv,"qd:h:",["datestamp=","hour="])
    except getopt.GetoptError:
        print 'region_summary_hour_repair.py -d <datestamp> -h <hour>'
        sys.exit(2)

    hour =''
    datestamp = ''

    for opt, arg in opts:
        if opt == '-q':
            print 'region_summary_hour_repair.py -d <datestamp> -h <hour>'
            sys.exit()
        elif opt in ("-d", "--datestamp"):
            datestamp = arg
        elif opt in ("-h", "--hour"):
            hour = arg

    print 'datestamp = %s' % datestamp
    print 'hour = %s' % hour

    return

    ts = calendar.timegm(time.gmtime())
    print "###################"
    print "# Performing the hourly mrqos_region summary"
    print "# starting processing time is " + str(ts) + " = " + time.strftime('GMT %Y-%m-%d %H:%M:%S', time.gmtime(ts))
    print "###################"
    ts_last_hour = ts-3600
    datestamp = time.strftime('%Y%m%d', time.gmtime(float(ts_last_hour)))
    hourstamp = time.strftime('%H', time.gmtime(float(ts_last_hour)))
    #hour_list = [str("%02d" % x) for x in range(24)]
    region_summary_retrial_max = 10

    # ############################### #
    # The SUMMARY HOUR hive procedure #
    # ############################### #
    print "    ****  summary hour tour:"
    # check if the summary has been performed on this particular hour (last hour)
    print "    ****  checking day = %s, hour = %s." % (datestamp, hourstamp),
    if hdfsutil.test_file(os.path.join(config.hdfs_qos_rg_hour % (datestamp, hourstamp), '000000_0.deflate')):
        print " file not exits,",
        f = open(os.path.join(config.mrqos_hive_query, 'mrqos_region_summarize_hour.hive'), 'r')
        strcmd = f.read()
        strcmd_s = strcmd % (datestamp, hourstamp, datestamp, hourstamp, datestamp, hourstamp)
        f.close()
        strcmd_g = "SELECT maprule, geoname, netname, region, avg_region_score, score_target, hourly_region_nsd_demand, hourly_region_eu_demand, hourly_region_ra_load, case_ra_load, case_nsd_demand, case_eu_demand, case_uniq_region, name, ecor, continent, country, city, latitude, longitude, provider, region_capacity, ecor_capacity, prp, numghosts, datestamp, hour FROM mrqos.mrqos_region_hour WHERE datestamp=%s and hour=%s;" % (datestamp, hourstamp)
        query_result_file = os.path.join(config.mrqos_query_result,'region_summary_hour.%s.%s.csv' % (datestamp, hourstamp))

        print " BLN for hourly summary: day = %s, hour = %s. " %(datestamp, hourstamp)
        count_retrial = 0
        while count_retrial < region_summary_retrial_max:
            tic = time.time()
            try:
                beeline.bln_e(strcmd_s)
                print "    ******  success with time cost = %s." % str(time.time()-tic)
                #try:
                #    beeline.bln_e_output(strcmd_g, query_result_file)
                #except:
                #    print "    ****  copy to local failed, retry!"
                #    beeline.bln_e_output(strcmd_g, query_result_file)
                break
            except:
                # delete the folder if summarization failed.
                print "    ******  failed with time cost = %s upto # retrials=%s" % (str(time.time()-tic), str(count_retrial))
                hdfsutil.rm(config.hdfs_qos_rg_hour % (datestamp, hourstamp), r=True)
                count_retrial += 1
    else:
        print " file exists."


    # ############################ #
    # The CASE VIEW hive procedure #
    # ############################ #
    print "    ****  case view tour:"
    # check if the summary has been performed on this particular hour (last hour)
    print "    ****  checking day = %s, hour = %s." % (datestamp, hourstamp),
    if hdfsutil.test_file(os.path.join(config.hdfs_qos_case_view_hour % (datestamp, hourstamp), '000000_0.deflate')):
        print " file not exits,",
        f = open(os.path.join(config.mrqos_hive_query, 'mrqos_case_view_hour.hive'), 'r')
        strcmd = f.read()
        strcmd_s = strcmd % (datestamp, hourstamp, datestamp, hourstamp)
        f.close()
        strcmd_g = "select * from mrqos.case_view_hour where datestamp=%s and hour=%s;" % (datestamp, hourstamp)
        query_result_file = os.path.join(config.mrqos_query_result,'case_view_hour.%s.%s.csv' % (datestamp, hourstamp))
        print " BLN for hourly summary for day = %s, hour = %s." % (datestamp, hourstamp)
        count_retrial = 0
        while count_retrial < region_summary_retrial_max:
            try:
                tic = time.time()
                beeline.bln_e(strcmd_s)
                print "    ******  success with time cost = %s." % str(time.time()-tic)
                try:
                    beeline.bln_e_output(strcmd_g, query_result_file)
                except:
                    print "    ****  copy to local failed, retry!"
                    beeline.bln_e_output(strcmd_g, query_result_file)
                break
            except:
                # delete the folder if summarization failed.
                print "    ******  failed with time cost = %s upto #retrials=%s" % (str(time.time()-tic), str(count_retrial))
                hdfsutil.rm(config.hdfs_qos_case_view_hour % (datestamp, hourstamp), r=True)
                count_retrial += 1

    else:
        print " file exists."


    # ############################## #
    # The REGION VIEW hive procedure #
    # ############################## #
    print "    ****  region view tour:"
    # check if the summary has been performed on this particular hour (last hour)
    print "    ****  checking day = %s, hour = %s." % (datestamp, hourstamp),
    if hdfsutil.test_file(os.path.join(config.hdfs_qos_rg_view_hour % (datestamp, hourstamp), '000000_0.deflate')):
        print " file not exits,",
        f = open(os.path.join(config.mrqos_hive_query, 'mrqos_region_view_hour.hive'), 'r')
        strcmd = f.read()
        strcmd_s = strcmd % (datestamp, hourstamp, datestamp, hourstamp, datestamp, hourstamp)
        f.close()
        strcmd_g = "select * from mrqos.region_view_hour where datestamp=%s and hour=%s;" % (datestamp, hourstamp)
        query_result_file = os.path.join(config.mrqos_query_result,'region_view_hour.%s.%s.csv' % (datestamp, hourstamp))
        print " BLN for hourly summary for day = %s, hour = %s." % (datestamp, hourstamp)
        count_retrial = 0
        while count_retrial < region_summary_retrial_max:
            try:
                tic = time.time()
                beeline.bln_e(strcmd_s)
                print "    ******  success with time cost = %s." % str(time.time()-tic)
                try:
                    beeline.bln_e_output(strcmd_g, query_result_file)
                except:
                    print "    ****  copy to local failed, retry!"
                    beeline.bln_e_output(strcmd_g, query_result_file)
                break
            except:
                # delete the folder if summarization failed.
                print "    ******  failed with time cost = %s upto #retrials=%s" % (str(time.time()-tic), str(count_retrial))
                hdfsutil.rm(config.hdfs_qos_rg_view_hour % (datestamp, hourstamp), r=True)
                count_retrial += 1

    else:
        print " file exists."


def cleanup_mrqos_region_related_tables(datestamp, hour):
    tables = ['mrqos_region_hour', 'case_view_hour', 'region_view_hour']
    for table_item in tables:
        try:
            # drop partitions (ok even if partition does not exist)
            hiveql_str = 'use mrqos; alter table %s drop if exists partition(datestamp=%s, hour=%s)' % (table_item,
                                                                                                        str(datestamp),
                                                                                                        str(hour))
            beeline.bln_e(hiveql_str)
            # remove data from HDFS (ok even if folder in hdfs does not exist)
            hdfs_d = os.path.join(config.hdfs_table, table_item, 'datestamp=%s' % str(datestamp), 'hour=%s' % str(hour))
            hdfsutil.rm(hdfs_d, r=True)
        except sp.CalledProcessError:
            print ">> failed in hive table clean up in table: %s for partition datestamp=%s, hour=%s." % (table_item,
                                                                                                          str(datestamp),
                                                                                                          str(hour))
            pass

# ==============================================================================
# # remove partitions from hive table
# ==============================================================================
#def mrqos_region_cleanup():
#    """ when called, this function will delete all partitions
#        the mrqos_region table as long as it is older than the threshold """



if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))