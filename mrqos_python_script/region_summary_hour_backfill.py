#!//usr/bin/python
"""
Created on Thu March 09 12:47:15 2016

@author: ychang
"""
import sys, os
#import shutil
import subprocess as sp

sys.path.append('/home/testgrp/MRQOS/')
import time
import calendar
import configurations.config as config
import configurations.hdfsutil as hdfsutil
import configurations.beeline as beeline
import logging

def main():
    """ get the date and hour for the previous hour. Will check from the beginning of the day, insert when missing. """
    ts = calendar.timegm(time.gmtime())
    logging.basicConfig(filename=os.path.join(config.mrqos_logging, 'cron_region_summary_hour.log'),
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S')
    logger = logging.getLogger(__name__)

    # start the logging
    logger.info("###################")
    logger.info("# Performing the hourly mrqos_region summary")
    logger.info("# starting time: " + str(ts) + " = " + time.strftime('GMT %Y-%m-%d %H:%M:%S', time.gmtime(ts)))
    logger.info("###################")

    # parameter: backfilter length
    bf_length = config.region_summary_back_filling
    ts_last_couple_hour_list = [ts-(1+x)*3600 for x in range(bf_length)]

    for ts_last_hour in ts_last_couple_hour_list:
        datestamp = time.strftime('%Y%m%d', time.gmtime(float(ts_last_hour)))
        hourstamp = time.strftime('%H', time.gmtime(float(ts_last_hour)))
        region_summary_retrial_max = 10

        # ############################### #
        # The SUMMARY HOUR hive procedure #
        # ############################### #
        #logger.info("    ****  summary hour tour: checking day = %s, hour = %s." % (datestamp, hourstamp))
        # check if the summary has been performed on this particular hour (last hour)
        if hdfsutil.test_file(os.path.join(config.hdfs_qos_rg_hour % (datestamp, hourstamp), '000000_0.deflate')):
            logger.info("** region summary hour: checking day = %s, hour = %s, and file does not exist." % (datestamp,
                                                                                                            hourstamp))
            f = open(os.path.join(config.mrqos_hive_query, 'mrqos_region_summarize_hour.hive'), 'r')
            strcmd = f.read()
            strcmd_s = strcmd % (datestamp, hourstamp, datestamp, hourstamp, datestamp, hourstamp)
            f.close()

            count_retrial = 0
            while count_retrial < region_summary_retrial_max:
                tic = time.time()
                try:
                    beeline.bln_e(strcmd_s)
                    logger.info("BLN region summary hour success @ cost = %s sec." % str(time.time()-tic))
                    break
                except sp.CalledProcessError as e:
                    # delete the folder if summarization failed.
                    logger.info("BLN region summary hour failed @ cost = %s sec in retrial #%s" % (str(time.time()-tic),
                                                                                                   str(count_retrial)))
                    logger.exception("message")
                    hdfsutil.rm(config.hdfs_qos_rg_hour % (datestamp, hourstamp), r=True)
                    count_retrial += 1
        else:
            logger.info("** region summary hour: checking day = %s, hour = %s, and file exists." % (datestamp,
                                                                                                    hourstamp))


        # ############################ #
        # The CASE VIEW hive procedure #
        # ############################ #
        #print "    ****  case view tour:"
        # check if the summary has been performed on this particular hour (last hour)
        if hdfsutil.test_file(os.path.join(config.hdfs_qos_case_view_hour % (datestamp, hourstamp), '000000_0.deflate')):
            logger.info("** case view hour: checking day = %s, hour = %s, and file does not exist." % (datestamp,
                                                                                                       hourstamp))
            f = open(os.path.join(config.mrqos_hive_query, 'mrqos_case_view_hour.hive'), 'r')
            strcmd = f.read()
            strcmd_s = strcmd % (datestamp, hourstamp, datestamp, hourstamp)
            f.close()
            strcmd_g = "select * from mrqos.case_view_hour where datestamp=%s and hour=%s;" % (datestamp, hourstamp)
            query_result_file = os.path.join(config.mrqos_query_result,'case_view_hour.%s.%s.csv' % (datestamp, hourstamp))

            count_retrial = 0
            while count_retrial < region_summary_retrial_max:
                try:
                    tic = time.time()
                    beeline.bln_e(strcmd_s)
                    logger.info("BLN case view hour success @ cost = %s sec." % str(time.time()-tic))
                    try:
                        beeline.bln_e_output(strcmd_g, query_result_file)
                    except sp.CalledProcessError as e:
                        logger.warning("copy to local failed, retrying...")
                        print e.message
                        try:
                            beeline.bln_e_output(strcmd_g, query_result_file)
                        except sp.CalledProcessError as e:
                            logger.error("copy to local failed again, abort.")
                            logger.exception("message")
                    break
                except sp.CalledProcessError as e:
                    # delete the folder if summarization failed.
                    logger.info("BLN case view hour failed @ cost = %s sec in retrial #%s" % (str(time.time()-tic),
                                                                                              str(count_retrial)))
                    logger.exception("message")
                    hdfsutil.rm(config.hdfs_qos_case_view_hour % (datestamp, hourstamp), r=True)
                    count_retrial += 1

        else:
            logger.info("** case view hour: checking day = %s, hour = %s, and file exists." % (datestamp,
                                                                                               hourstamp))



        # ############################## #
        # The REGION VIEW hive procedure #
        # ############################## #
        # check if the summary has been performed on this particular hour (last hour)
        if hdfsutil.test_file(os.path.join(config.hdfs_qos_rg_view_hour % (datestamp, hourstamp), '000000_0.deflate')):
            logger.info("** region view hour: checking day = %s, hour = %s, and file does not exist." % (datestamp,
                                                                                                         hourstamp))
            f = open(os.path.join(config.mrqos_hive_query, 'mrqos_region_view_hour.hive'), 'r')
            strcmd = f.read()
            strcmd_s = strcmd % (datestamp, hourstamp, datestamp, hourstamp, datestamp, hourstamp)
            f.close()
            strcmd_g = "select * from mrqos.region_view_hour where datestamp=%s and hour=%s;" % (datestamp, hourstamp)
            query_result_file = os.path.join(config.mrqos_query_result,'region_view_hour.%s.%s.csv' % (datestamp, hourstamp))

            count_retrial = 0
            while count_retrial < region_summary_retrial_max:
                try:
                    tic = time.time()
                    beeline.bln_e(strcmd_s)
                    logger.info("BLN region view hour success @ cost = %s sec." % str(time.time()-tic))
                    try:
                        beeline.bln_e_output(strcmd_g, query_result_file)
                    except sp.CalledProcessError as e:
                        logger.warning("copy to local failed, retrying...")
                        print e.message
                        try:
                            beeline.bln_e_output(strcmd_g, query_result_file)
                        except sp.CalledProcessError as e:
                            logger.error("copy to local failed again, abort.")
                            logger.exception("message")
                    break
                except sp.CalledProcessError as e:
                    # delete the folder if summarization failed.
                    logger.info("BLN region view hour failed @ cost = %s sec in retrial #%s" % (str(time.time()-tic),
                                                                                                str(count_retrial)))
                    logger.exception("message")
                    hdfsutil.rm(config.hdfs_qos_rg_view_hour % (datestamp, hourstamp), r=True)
                    count_retrial += 1

        else:
            logger.info("** region view hour: checking day = %s, hour = %s, and file exists." % (datestamp,
                                                                                                 hourstamp))




# ==============================================================================
# # remove partitions from hive table
# ==============================================================================
#def mrqos_region_cleanup():
#    """ when called, this function will delete all partitions
#        the mrqos_region table as long as it is older than the threshold """



if __name__ == '__main__':
    sys.exit(main())