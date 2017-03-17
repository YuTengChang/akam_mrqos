#!/a/bin/python2.7
"""
Created on Thu Apr 16 16:47:15 2015

@author: ychang
"""
import sys, os
import shutil

sys.path.append('/home/testgrp/MRQOS/')
import subprocess as sp
import datetime
import YT_Timeout as ytt
import configurations.config as config
import configurations.hdfsutil as hdfsutil
import configurations.beeline as beeline
import glob
import logging


def main():
    """  this function will do the query on 5 different measurement and upload
    the data to hdfs accordingly, this also join tables at single time point """

    # different queries (various types)
    # logging set-up
    logging.basicConfig(filename=os.path.join(config.mrqos_logging, 'mrqos_region_pref_allowlist.log'),
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S')
    logger = logging.getLogger(__name__)

    # ##############################
    # start the script
    # parameter setting
    # ##############################
    # change the ioratio to new version
    mtype = ['mcm_regionPreferences']

    datenow = str(datetime.date.today())
    datenow = datenow[:4] + datenow[6:8] + datenow[9:]

    sql = """sql2 -q map.devbl.query.akadns.net --csv "`cat """
    post = """`" | tail -n+3 > """

    minimum_return_lines = 40000
    # fetch the data through query with retrials
    logger.info('query data from agg...')
    n_retrial = config.query_retrial
    t_timeout = config.query_timeout
    for item in mtype:
        flag = 0
        count = 0
        dest = os.path.join(config.mrqos_data, item + '.tmp1')
        aggs = os.path.join(config.mrqos_query, item + '.qr')
        cmd = sql + aggs + post + dest

        # multiple times with timeout scheme
        while (flag == 0) and (count < n_retrial):
            try:
                with ytt.Timeout(t_timeout):
                    sp.check_call(cmd, shell=True)
                    # in case return empty result.
                    if (int(sp.check_output('wc -l %s' % os.path.join(config.mrqos_data,
                                                                      '%s.tmp1' % item),
                                            shell=True).split()[0]) > minimum_return_lines):
                        flag = 1
                    elif (int(sp.check_output('wc -l %s' % os.path.join(config.mrqos_data,
                                                                      '%s.tmp1' % item),
                                              shell=True).split()[0]) > 0):
                        count += 1
                        logger.info('partially-returned table %s at re-try count = %s' % (item, str(count)))
                        print "partially returned at count=%s" % str(count)
                    else:
                        count += 1
                        logger.info('empty table %s at re-try count = %s' % (item, str(count)))
                        print "empty at count=%s" % str(count)
            except:
                count += 1
                logger.info('timeout table %s at re-try count = %s' % (item, str(count)))
                print "timeout at count = %s" % str(count)
        # if any of the query not fetched successfully, break all and stop running
        if count >= n_retrial:
            logger.info('data query fetch failed for table %s.' % item)
            return

    # ########################## #
    # uploading the queried data #
    # ########################## #

    filelist = sorted(glob.glob(os.path.join(config.mrqos_data,
                                             'mcm_regionPreferences.tmp1')))

    for fileitem in filelist:
        this_ts = fileitem.split('.')[-2]
        filename = fileitem.split('/')[-1]
        hdfs_d = os.path.join(config.hdfs_table,
                              'mcm_region_pref',
                              'datestamp=%s' % datenow)
        try:
            logger.info('trying upload and alter for file: %s' % filename)
            beeline.upload_to_hive(fileitem,
                                   hdfs_d,
                                   'datestamp=%s' % datenow,
                                   'mcm_region_pref',
                                   logger)
            # remove local file
            os.remove(fileitem)
            logger.info('remove local: %s' % filename)
        except sp.CalledProcessError as e:
            logger.error('upload to hdfs & alter hive table failed for file: %s' % fileitem)
            logger.error('error message: %s', e.message)

    # TODO: upload the joined to HDFS
    # TODO: clean up the joined

if __name__ == '__main__':
    sys.exit(main())
