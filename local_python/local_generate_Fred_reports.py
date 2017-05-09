#!/opt/anaconda/bin/python
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 15:58:55 2015

@author: ychang

This script do the distance calculation

"""
import sys,os
import math
import numpy
import time
import pandas as pd
from datetime import date, timedelta
import subprocess as sp
import locale

def main():
    # current time. Labeled as yesterday because the summarization ends D-1
    locale.setlocale(locale.LC_ALL, 'en_US')
    timenow = int(time.time())
    today = date.today() - timedelta(1)

    # parameters:
    headers = ['startdate','enddate','maprule','geoname','netname','sp95_t95',
            'sp95_t90','sp75_t95','sp75_t90','dp95_t95','dp95_t90','dp75_t95',
            'dp75_t90','ocy_t95','ocy_t90','oct_t95','oct_t90',
            'io_t95','io_t90','load',
            'sp95_t95_2w','sp95_t90_2w','sp75_t95_2w','sp75_t90_2w',
            'dp95_t95_2w','dp95_t90_2w','dp75_t95_2w','dp75_t90_2w',
            'ocy_t95_2w','ocy_t90_2w','oct_t95_2w','oct_t90_2w',
            'io_t95_2w','io_t90_2w','load_2w','mr',
            'Service','Group','mrname','priority']

    map_idx = [332,2903,1,4991,290,2905,2910,2923,121,122,9636,9637,
                9638,9639,9631,9632,9633,9634,4992,9644,9645,9646,
                168,487,108,11,4658,489,2969,156,2968,2939,4657,9690,9691,6,2935,
                4666,4667,4668,4669,2948,82,436,382,94,12,399,400,2527,28,29,93,92,91,5695,30,390,443,
                127,26,27,18,4386,36,4382,4383,4384,4385,
                2510,2680,2685,2686,4662,81,443,
                2919,2964,2965,106,9440,9441,9442,9443,9438,
                251,252,2716,2717,2718,2719,2720,387,2790,2791,2792,2793,2794,2795,2796,2797,2798,2799,7646,
                4906,4907,4908,4909,4910,4911,4912,4913,4914,4915,11297,11298,11299,4937,4938,4939,2971,
                11200,11201,11202,11203,11204,11205,11206,11207,11208,11209,11210,11211,11212,
                11213,11214,11215,11216,11217,11218,11219,11220,11221,11222,11223,11224,
                11225,11226,11227,11228,11229,11230,11231,11232,11233,11234,11235,11236,11237,
                11238,11239,11240,11241,11242,11243,11244,11245,11246,11247,11248,11249,11520,
                7660,7661,7662,7663,7664,7665,7666,7667,7668,7669,7647,7648,7950,7951,7952,7953,7954,7955,7956,7957]

    filedir = '/var/www/txt'
    dictionary_file = '/u4/ychang/Projects/18-MRQOS/Data/mr_final_dictionary.txt'

    ## 2W Report Summary
    # generate the dictionary-looked-up file, append the date
    filename = 'processed_2wjoin_full_wloads_wio.csv'
    reportname = 'Fred2WReport.%s.csv' % (str(today))

    file_source =  os.path.join(filedir, reportname)

    cmd_str = ''' awk -F, 'FNR==NR{a[$1]=$0;next}{print $0","a[$3]}' %s %s | awk -F, 'NF>37' > %s ''' % (dictionary_file,
                         os.path.join(filedir, filename),
                         os.path.join(filedir, reportname))

    sp.check_call(cmd_str, shell=True)

    data = numpy.genfromtxt(file_source,delimiter=',', dtype='str')

    dff = pd.DataFrame(data,columns=headers)
    df = dff.convert_objects(convert_numeric=True)

    df.load_2w = [ str(int(round((x-y)/y*100,0))) if y>0 else '0' for (x,y) in zip(df.load, df.load_2w) ]
    df.load = [ str(x)+' ('+y+'%)' for (x,y) in zip(df.load, df.load_2w) ]

    df = df.loc[:,['startdate','enddate','Service','Group','mrname','priority',
            'maprule','geoname','netname',
            'sp95_t95','sp75_t95','ocy_t95','io_t95','load',
            'sp95_t95_2w','sp75_t95_2w','ocy_t95_2w','io_t95_2w']]

    #df2 = df[df.maprule.isin(map_idx) & df.geoname.isin(inGeo) & df.netname.isin(['ANY'])];
    df2 = df[df.maprule.isin(map_idx) & df.netname.isin(['ANY'])]

    df2.to_csv(os.path.join(filedir,reportname), header=False, index=False)

    ## 3D Report Summary
    # generate the dictionary-looked-up file, append the date
    filename = 'processed_3djoin_full_wloads_wio.csv'
    reportname = 'Fred3DReport.%s.csv' % (str(today))

    file_source =  os.path.join(filedir, reportname)

    cmd_str = ''' awk -F, 'FNR==NR{a[$1]=$0;next}{print $0","a[$3]}' %s %s | awk -F, 'NF>37' > %s ''' % (dictionary_file,
                         os.path.join(filedir, filename),
                         os.path.join(filedir, reportname))

    sp.check_call(cmd_str, shell=True)

    data = numpy.genfromtxt(file_source,delimiter=',', dtype='str')

    dff = pd.DataFrame(data, columns=headers)
    df = dff.convert_objects(convert_numeric=True)

    df.load_2w = [ str(int(round((x-y)/y*100,0))) if y>0 else '0' for (x,y) in zip(df.load, df.load_2w) ]
    df.load = [ str(x)+' ('+y+'%)' for (x,y) in zip(df.load, df.load_2w) ]

    df = df.loc[:,['startdate','enddate','Service','Group','mrname','priority',
            'maprule','geoname','netname',
            'sp95_t95','sp75_t95','ocy_t95','io_t95','load',
            'sp95_t95_2w','sp75_t95_2w','ocy_t95_2w','io_t95_2w']]

    #df2 = df[df.maprule.isin(map_idx) & df.geoname.isin(inGeo) & df.netname.isin(['ANY'])];
    df2 = df[df.maprule.isin(map_idx) & df.netname.isin(['ANY'])]

    df2.to_csv(os.path.join(filedir,reportname), header=False, index=False)

if __name__ == '__main__':
    main()
