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

def main():
    query_item = ['maprule_info', 'mcm_machines']
    agg = 'mega.dev.query.akadns.net'
    for item in query_item:
        cmd = ''' sql2 -q %s --csv "`cat %s.qr`" > %s.tmp ''' % (agg,
                                                                 os.path.join(config.mrqos_query, item),
                                                                 os.path.join(config.mrqos_data, item))
        count = 0
        flag = 0
        n_retrial = config.query_retrial
        t_timeout = config.query_timeout * 2 # 40 seconds
        # multiple times with timeout scheme
        while (flag == 0) and (count < n_retrial):
            try:
                with ytt.Timeout(t_timeout):
                    sp.call(cmd, shell=True)
                    flag = 1
            except:
                count += 1


if __name__ == '__main__':
    sys.exit(main())