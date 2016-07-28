#!/home/ychang/anaconda/bin/python2.7
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


def main():
    """  this function will do the query on 5 different measurement and upload
    the data to hdfs accordingly, this also join tables at single time point """

    # different queries (various types)
    mtype = ['score', 'distance', 'in_country', 'in_continent', 'ra_load']

    sql = """sql2 -q map.mapnoccthree.query.akadns.net --csv "`cat """
    post = """`" | tail -n+3 | awk -F"," 'BEGIN{OFS=","}{$1=""; print $0}' | sed 's/^,//g' > """

    # current time
    timenow = int(time.time())

    print "###################"
    print "Start processing the data back in for 10 minute joins"
    print "starting processing time is " + str(timenow)
    print "###################"

    # fetch the data through query with retrials
    print "    ****  querying mrqos data."
    for item in mtype:
        flag = 0
        count = 0
        dest = os.path.join(config.mrqos_data, item + '.tmp')
        aggs = os.path.join(config.mrqos_query, item + '.qr')

        cmd = sql + aggs + post + dest
        n_retrial = config.query_retrial
        t_timeout = config.query_timeout
        # multiple times with timeout scheme
        while (flag == 0) and (count < n_retrial):
            try:
                with ytt.Timeout(t_timeout):
                    sp.call(cmd, shell=True)
                    flag = 1
            except:
                count += 1
        # if any of the query not fetched successfully, break all and stop running
        if count >= n_retrial:
            print ">> data fetch failed in querying table %s" % item
            return

    # provide SCORE table with peak/off-peak attribute
    print "    ****  provide PEAK in score."
    sp.call([config.provide_peak], shell=True)

    # backup the individual query file by copying to backup folder
    print "    ****  backing up queried results."
    if not os.path.exists('/home/testgrp/MRQOS/mrqos_data/backup/%s' % str(timenow)):
        os.makedirs('/home/testgrp/MRQOS/mrqos_data/backup/%s' % str(timenow))
        for item in mtype:
            filesrc = os.path.join(config.mrqos_data, item + '.tmp')
            filedst = '/home/testgrp/MRQOS/mrqos_data/backup/%s/' % str(timenow)
            shutil.copy(filesrc, filedst)

    # load the result to python pandas
    filedir = config.mrqos_data
    filelist = ['score.tmp','distance.tmp','in_country.tmp','in_continent.tmp','ra_load.tmp']

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

    dfscore = pd.DataFrame(data,columns=header)
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

    dfdistance = pd.DataFrame(data,columns=header)
    dfdistance.index = dfdistance.casename

    file_source = os.path.join(filedir, filelist[2])
    data = numpy.genfromtxt(file_source, delimiter=',', skip_header=0, dtype='str')
    header = ['casename',
              'maprule',
              'geoname',
              'netname',
              'icy',
              'icytarget']

    dficy = pd.DataFrame(data,columns=header)
    dficy.index = dficy.casename

    file_source =  os.path.join(filedir, filelist[3])
    data = numpy.genfromtxt(file_source, delimiter=',', skip_header=0, dtype='str')
    header = ['casename',
              'maprule',
              'geoname',
              'netname',
              'ict',
              'icttarget']

    dfict = pd.DataFrame(data,columns=header)
    dfict.index = dfict.casename

    file_source =  os.path.join(filedir, filelist[4])
    data = numpy.genfromtxt(file_source, delimiter=',', skip_header=0, dtype='str')
    header = ['casename',
              'maprule',
              'geoname',
              'netname',
              'ra_load',
              'ra_pingbased',
              'ping_ratio']

    dfra = pd.DataFrame(data,columns=header)
    dfra.index = dfra.casename

    df2 = dfscore.join(dfdistance, rsuffix='_dis', how='inner')\
            .join(dficy, rsuffix='_icy', how='inner')\
            .join(dfict, rsuffix='_ict', how='inner')\
            .join(dfra, rsuffix='_ra', how='inner')

    dropped_columns = [x for x in df2.columns if '_dis' in x or '_icy' in x or '_ict' in x or '_ra' in x]
    print dropped_columns
    df2.drop(dropped_columns, axis=1, inplace=True)
    df2.reset_index(drop=True, inplace=True)

    output_name = os.path.join('/home/testgrp/MRQOS/mrqos_data/backup/%s' % str(timenow),
                               'mrqos_join.%s.csv' % str(timenow))
    df2.to_csv(output_name,
               sep='\t', index=False, header=False)

if __name__ == '__main__':
    sys.exit(main())
