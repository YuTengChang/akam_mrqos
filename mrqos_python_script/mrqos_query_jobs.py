#!/a/bin/python2.7
"""
Created on Thu Apr 16 16:47:15 2015

@author: ychang
"""
import sys, os
import shutil

sys.path.append('/home/testgrp/MRQOS/')
import subprocess as sp
import pandas as pd
import numpy
import time
import YT_Timeout as ytt
import configurations.config as config
import configurations.hdfsutil as hdfsutil
import configurations.beeline as beeline
import glob
import logging

def calDef(row, col1, col2):
    if row[col1] >= row[col2]:
        val = row[col1] - row[col2]
    else:
        val


def main():
    """  this function will do the query on 5 different measurement and upload
    the data to hdfs accordingly, this also join tables at single time point """

    # different queries (various types)
    # logging set-up
    logging.basicConfig(filename=os.path.join(config.mrqos_logging, 'mrqos_query.log'),
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S')
    logger = logging.getLogger(__name__)

    # ##############################
    # start the script
    # parameter setting
    # ##############################

    mtype = ['score', 'distance', 'in_country', 'in_continent', 'ra_load', 'in_out_ratio']

    sql = """sql2 -q map.mapnoccthree.query.akadns.net --csv "`cat """
    post = """`" | tail -n+3 | awk -F"," 'BEGIN{OFS=","}{$1=""; print $0}' | sed 's/^,//g' > """

    # current time
    timenow = int(time.time())
    logger.info('###################')
    logger.info('Start processing the data back in for 10 minute joins')
    logger.info('starting processing time is %s' % str(timenow))
    logger.info('###################')

    # fetch the last backup'ed folder
    backups = sorted(glob.glob(os.path.join(config.mrqos_data_backup, '1*')))
    if len(backups) > 0:
        backup_folders = backups[-1]
        logger.info('backup files in folder: %s' % backup_folders)
    else:
        backup_folders = ''
        logger.warning('no backup files')

    # fetch the data through query with retrials
    logger.info('query data from agg...')
    n_retrial = config.query_retrial
    t_timeout = config.query_timeout
    for item in mtype:
        flag = 0
        count = 0
        dest = os.path.join(config.mrqos_data, item + '.tmp')
        aggs = os.path.join(config.mrqos_query, item + '.qr')

        cmd = sql + aggs + post + dest
        # for in_out_ratio allow larger query time
        if item == 'in_out_ratio':
            t_timeout = t_timeout*2-1

        # multiple times with timeout scheme
        while (flag == 0) and (count < n_retrial):
            try:
                with ytt.Timeout(t_timeout):
                    sp.call(cmd, shell=True)
                    # in case return empty result.
                    if (int(sp.check_output('wc -l %s' % os.path.join(config.mrqos_data,
                                                                      '%s.tmp' % item),
                                            shell=True).split()[0]) > 0):
                        flag = 1
                    else:
                        count += 1
            except:
                count += 1
                print "count = %s" % str(count)
#####
while (flag == 0) and (count < n_retrial):
    try:
        with ytt.Timeout(t_timeout):
            time.sleep(4-count)
            flag = 1
    except:
        count += 1
        print "count = %s" % str(count)

#####
        # if any of the query not fetched successfully, break all and stop running
        if count >= n_retrial:
            logger.info('data query fetch failed for table %s.' % item)
            # copy from the past
            if backup_folders:
                logger.info('copy from the backup folder, which labels ts=%s' % backup_folders.split('/')[-1])
                if item == 'score':
                    shutil.copy(os.path.join(backup_folders, 'score1.tmp'),
                                os.path.join(config.mrqos_data, 'score.tmp'))
                else:
                    shutil.copy(os.path.join(backup_folders, item+'.tmp'),
                                os.path.join(config.mrqos_data, item+'.tmp'))
            # if no backup to copy from, quit.
            else:
                logger.error('no backup this time. Stop the query.')
                if item != 'in_out_ratio':
                    return

    # provide SCORE table with peak/off-peak attribute
    logger.info('provide PEAK in table "score".')
    sp.call([config.provide_peak], shell=True)

    # backup the individual query file by copying to backup folder
    logger.info('backing up individual queried results.')
    if not os.path.exists('/home/testgrp/MRQOS/mrqos_data/backup/%s' % str(timenow)):
        os.makedirs('/home/testgrp/MRQOS/mrqos_data/backup/%s' % str(timenow))
        for item in mtype+['score1']:
            filesrc = os.path.join(config.mrqos_data, item + '.tmp')
            filedst = '/home/testgrp/MRQOS/mrqos_data/backup/%s/' % str(timenow)
            shutil.copy(filesrc, filedst)

    # load the result to python pandas
    filedir = config.mrqos_data
    filelist = ['score.tmp','distance.tmp','in_country.tmp','in_continent.tmp','ra_load.tmp','in_out_ratio.tmp']

    file_source = os.path.join(filedir, filelist[0])
    data = numpy.genfromtxt(file_source, delimiter=',', skip_header=0, dtype='str')
    header = ['casename',
              'maprule',
              'geoname',
              'netname',
              'sp99',
              'sp95',
              'sp90',
              'sp75',
              'starget',
              'ispeak']

    dfscore = pd.DataFrame(data, columns=header)
    dfscore['sp99d'] = [z*(z>0) for z in [int(x)-int(y) for (x,y) in zip(dfscore.sp99, dfscore.starget)]]
    dfscore['sp95d'] = [z*(z>0) for z in [int(x)-int(y) for (x,y) in zip(dfscore.sp95, dfscore.starget)]]
    dfscore['sp90d'] = [z*(z>0) for z in [int(x)-int(y) for (x,y) in zip(dfscore.sp90, dfscore.starget)]]
    dfscore['sp75d'] = [z*(z>0) for z in [int(x)-int(y) for (x,y) in zip(dfscore.sp75, dfscore.starget)]]
    dfscore.index = dfscore.casename

    file_source = os.path.join(filedir, filelist[1])
    data = numpy.genfromtxt(file_source, delimiter=',', skip_header=0, dtype='str')
    header = ['casename',
              'maprule',
              'geoname',
              'netname',
              'dp99',
              'dp95',
              'dp90',
              'dp75',
              'dtarget']

    dfdistance = pd.DataFrame(data, columns=header)
    dfdistance['dp99d'] = [z*(z>0) for z in [int(x)-int(y) for (x,y) in zip(dfdistance.dp99, dfdistance.dtarget)]]
    dfdistance['dp95d'] = [z*(z>0) for z in [int(x)-int(y) for (x,y) in zip(dfdistance.dp95, dfdistance.dtarget)]]
    dfdistance['dp90d'] = [z*(z>0) for z in [int(x)-int(y) for (x,y) in zip(dfdistance.dp90, dfdistance.dtarget)]]
    dfdistance['dp75d'] = [z*(z>0) for z in [int(x)-int(y) for (x,y) in zip(dfdistance.dp75, dfdistance.dtarget)]]
    dfdistance.index = dfdistance.casename

    file_source = os.path.join(filedir, filelist[2])
    data = numpy.genfromtxt(file_source, delimiter=',', skip_header=0, dtype='str')
    header = ['casename',
              'maprule',
              'geoname',
              'netname',
              'icy',
              'icytarget']

    dficy = pd.DataFrame(data, columns=header)
    dficy['icyd'] = [z*(z>0) for z in [int(y)-int(x) for (x,y) in zip(dficy.icy, dficy.icytarget)]]
    dficy.index = dficy.casename

    file_source = os.path.join(filedir, filelist[3])
    data = numpy.genfromtxt(file_source, delimiter=',', skip_header=0, dtype='str')
    header = ['casename',
              'maprule',
              'geoname',
              'netname',
              'ict',
              'icttarget']

    dfict = pd.DataFrame(data, columns=header)
    dfict['ictd'] = [z*(z>0) for z in [int(y)-int(x) for (x,y) in zip(dfict.ict, dfict.icttarget)]]
    dfict.index = dfict.casename

    file_source = os.path.join(filedir, filelist[4])
    data = numpy.genfromtxt(file_source, delimiter=',', skip_header=0, dtype='str')
    header = ['casename',
              'maprule',
              'geoname',
              'netname',
              'ra_load',
              'ra_pingbased',
              'ping_rtio']

    dfra = pd.DataFrame(data, columns=header)
    dfra.index = dfra.casename

    file_source = os.path.join(filedir, filelist[5])
    data = numpy.genfromtxt(file_source, delimiter=',', skip_header=0, dtype='str')
    header = ['casename',
              'ioratio',
              'iotarget',
              'coverage']

    dfio = pd.DataFrame(data, columns=header)
    dfio.index = dfio.casename

    df2 = dfscore.join(dfdistance, rsuffix='_dis', how='inner')\
            .join(dficy, rsuffix='_icy', how='inner')\
            .join(dfict, rsuffix='_ict', how='inner')\
            .join(dfra, rsuffix='_ra', how='inner')

    # drop redundant columns in df2
    dropped_columns = [x for x in df2.columns if '_dis' in x or '_icy' in x or '_ict' in x or '_ra' in x]
    df2.drop(dropped_columns, axis=1, inplace=True)

    df3 = df2.join(dfio, rsuffix='_ioratio')

    df2.drop(['casename'], axis=1, inplace=True)
    df2.reset_index(drop=True, inplace=True)
    df3.drop(['casename','casename_ioratio'], axis=1, inplace=True)
    df3.reset_index(drop=True, inplace=True)

    output_name = os.path.join('/home/testgrp/MRQOS/mrqos_data/backup/joined',
                               'mrqos_join.%s.csv' % str(timenow))
    df2.to_csv(output_name,
               sep='\t', index=False, header=False)
    output_name = os.path.join('/home/testgrp/MRQOS/mrqos_data/backup/joined',
                               'mrqos_joinv2.%s.csv' % str(timenow))
    df3.to_csv(output_name,
               sep='\t', index=False, header=False)

    # clean up backups
    if len(backups) > 0:
        for backup in backups:
            shutil.rmtree(backup)
            logger.info('remove old backup folders: %s' % backup)

    # TODO: upload the joined to HDFS
    # TODO: clean up the joined

if __name__ == '__main__':
    sys.exit(main())
