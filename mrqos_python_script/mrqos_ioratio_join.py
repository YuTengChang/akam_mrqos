#!/a/bin/python2.7
"""
Created on Thu Apr 16 16:47:15 2015

@author: ychang
"""
import sys, os
import shutil

sys.path.append('/home/testgrp/MRQOS/')
import subprocess as sp
import time
import YT_Timeout as ytt
import configurations.config as config
import configurations.hdfsutil as hdfsutil
import configurations.beeline as beeline
import logging

def main():
    # logging set-up
    logging.basicConfig(filename=os.path.join(config.mrqos_logging, 'io_ratio_prefetch.log'),
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S')
    logger = logging.getLogger(__name__)

    # ##############################
    # start the script
    # parameter setting

    ts = int(time.time())
    logger.info('########### ts=%s ###########' % str(ts))
    #datestamp = time.strftime('%Y%m%d', time.gmtime(float(ts)))
    #hourstamp = time.strftime('%H', time.gmtime(float(ts)))

    # IO-Ratio Join:
    last_mrqos_region_partition = beeline.get_last_partitions('mrqos.mrqos_region')
    [datestamp, hourstamp, ts_region] = [x.split('=')[1] for x in last_mrqos_region_partition.split('/')]
    logger.info('MRQOS mrqos_region partition: datestamp=%s, hour=%s, ts_region=%s' % (datestamp,
                                                                                 hourstamp,
                                                                                 ts_region))

    mapruleinfo_partitions = [x for x in sorted(beeline.show_partitions('mrqos.maprule_info').split('\n'),reverse=True) if '=' in x]
    mapruleinfo_partitions = [x for x in mapruleinfo_partitions if x < 'ts=%s' % ts_region]
    ts_mapruleinfo = mapruleinfo_partitions[0].split('=')[1]
    logger.info('MRQOS maprule_info partition: ts_mapruleinfo=%s' % ts_mapruleinfo)

    region_summary_retrial_max = 10

    # ############################### #
    # The In-Out Ratio hive procedure #
    # ############################### #
    # check if the summary has been performed on this particular hour (last hour)
    # print "    ****  checking day = %s, hour = %s." % (datestamp, hourstamp),
    if hdfsutil.test_file(os.path.join(config.hdfs_table,
                                       'mrqos_ioratio',
                                       'datestamp=%s' % datestamp,
                                       'hour=%s' % hourstamp,
                                       'ts=%s' % ts_region,
                                       '000000_0.deflate')):
        logger.info(' Joined file not exist.')
        f = open(os.path.join(config.mrqos_hive_query, 'mrqos_ioratio.hive'), 'r')
        strcmd = f.read()
        strcmd_s = strcmd % (datestamp, hourstamp, ts_region,
                             datestamp, hourstamp, ts_region,
                             ts_mapruleinfo)
        f.close()
        # strcmd_g = "SELECT maprule, geoname, netname, region, avg_region_score, score_target, hourly_region_nsd_demand, hourly_region_eu_demand, hourly_region_ra_load, case_ra_load, case_nsd_demand, case_eu_demand, case_uniq_region, name, ecor, continent, country, city, latitude, longitude, provider, region_capacity, ecor_capacity, prp, numghosts, datestamp, hour FROM mrqos.mrqos_region_hour WHERE datestamp=%s and hour=%s;" % (datestamp, hourstamp)
        # query_result_file = os.path.join(config.mrqos_query_result,'region_summary_hour.%s.%s.csv' % (datestamp, hourstamp))

        print " BLN for hourly summary: day = %s, hour = %s. " %(datestamp, hourstamp)
        count_retrial = 0
        while count_retrial < region_summary_retrial_max:
            tic = time.time()
            try:
                beeline.bln_e(strcmd_s)
                logger.info('    ******  success with time cost = %s.' % str(time.time()-tic))
                break
            except sp.CalledProcessError as e:
                # delete the folder if summarization failed.
                logger.error('    ******  failed with time cost = %s upto # retrials=%s' % (str(time.time()-tic), str(count_retrial)))
                logger.error('error %s' % e.message)
                hdfsutil.rm(os.path.join(config.hdfs_table,
                                         'mrqos_ioratio',
                                         'datestamp=%s' % datestamp,
                                         'hour=%s' % hourstamp,
                                         'ts=%s' % ts_region), r=True)
                count_retrial += 1
    else:
        logger.info(' Joined file exists.')


if __name__ == '__main__':
    sys.exit(main())