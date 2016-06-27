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
                        format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S  ')
    logger = logging.getLogger(__name__)

    # ##############################
    # start the script
    # parameter setting

    ts = int(time.time())
    datestamp = time.strftime('%Y%m%d', time.gmtime(float(ts)))
    hourstamp = time.strftime('%H', time.gmtime(float(ts)))

    logger.info('Fetch table(s) started at %s.' % str(ts))
    query_item = ['maprule_info', 'mcm_machines']
    agg = 'mega.dev.query.akadns.net'

    for item in query_item:
        cmd = ''' sql2 -q %s --csv "`cat %s.qr`" | tail -n+3 > %s.tmp ''' % (agg,
                                                                 os.path.join(config.mrqos_query, item),
                                                                 os.path.join(config.mrqos_data, item))
        count = 0
        flag = 0
        n_retrial = config.query_retrial
        t_timeout = config.query_timeout * 2 # 40 seconds
        # multiple times with timeout scheme
        while (flag == 0) and (count < n_retrial):
            try:
                tic = time.time()
                with ytt.Timeout(t_timeout):
                    sp.call(cmd, shell=True)
                    flag = 1
                    logger.info('%s trial: Fatched Table: %s with %s sec.' % (str(count+1),
                                                                              item,
                                                                              str(time.time()-tic) ))
            except Exception:
                count += 1
        if flag == 0:
            logger.info('Table %s fetched failed.' % item)

        # upload to hdfs and add hive partitions
        try:
            hdfs_d = os.path.join(config.hdfs_table,
                                    item,
                                    'ts=%s' % str(ts))

            beeline.upload_to_hive(os.path.join(config.mrqos_data, '%s.tmp' % item),
                                   hdfs_d,
                                   'ts=%s' % (str(ts)),
                                   'mrqos.%s' % item,
                                   logger)
            logger.info('successfully upload to HDFS and add partition: mrqos.%s' % item)

        except sp.CalledProcessError as e:
            logger.error('upload to hive and add partition failed: mrqos.%s' % item)
            logger.error('error: %s' % e.message)

        ## obtain the last partition
        #try:
        #    partitions = beeline.show_partitions('mrqos.%s' % item).split('\n')[-1].split('=')[1]
        #    exec('%s_par=partitions' % item)
        #
        #except sp.CalledProcessError as e:
        #    logger.error('fetch partition error, table: %s' % item)
        #    logger.error('error: %s' % e.message)






if __name__ == '__main__':
    sys.exit(main())