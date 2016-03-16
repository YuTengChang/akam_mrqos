#!//usr/bin/python
"""
Created on Thu March 09 12:47:15 2016

@author: ychang
"""
import sys, os
#import shutil

sys.path.append('/home/testgrp/MRQOS/')
import time
import configurations.config as config
import configurations.beeline as beeline



def main():

    datestamp = '20160316'
    hourstamp = '04'

    # test0 the original order of join
    f = open(os.path.join(config.mrqos_hive_query, 'test0_mrqos_region_view_hour.hive'), 'r')
    strcmd = f.read()
    strcmd_s1 = strcmd % (datestamp, hourstamp, datestamp, hourstamp)
    f.close()

    # test the reverse order of join
    f = open(os.path.join(config.mrqos_hive_query, 'test_mrqos_region_view_hour.hive'), 'r')
    strcmd = f.read()
    strcmd_s2 = strcmd % (datestamp, hourstamp, datestamp, hourstamp)
    f.close()

    fail_count = [0] * 2
    time_count = [0] * 2

    iter = 10

    for item in range(iter):
        tic = time.time()
        fail0 = False
        fail1 = False
        try:
            beeline.bln_e(strcmd_s1)
            span1 = time.time()-tic
            time_count[0] += span1
        except:
            span1 = time.time()-tic
            fail_count[0] += 1
            fail0 = True

        tic = time.time()
        try:
            beeline.bln_e(strcmd_s2)
            span2 = time.time()-tic
            time_count[1] += span2
        except:
            span2 = time.time()-tic
            fail_count[1] += 1
            fail1 = True

        print "test0 takes %s (%s) and test1 takes %s (%s)" % (str(span1),
                                                               "failed" if fail0 else "ok",
                                                               str(span2),
                                                               "failed" if fail1 else "ok")

    print "<<< overall result >>>"
    print "test0 takes %s and test1 takes %s" % (str(time_count[0]/(iter-fail_count[0])),
                                                 str(time_count[1]/(iter-fail_count[1])))

if __name__ == '__main__':
    sys.exit(main())
