#!/opt/anaconda/bin/python
"""
Created on Thu March 09 12:47:15 2016

@author: ychang
"""
import sys, os
import shutil
import time
import calendar
import subprocess as sp
sys.path.append('/home/ychang/Documents/Projects/18-DDC/MRQOS/')
import configurations.config as config

def main():
    tic = time.time()
    query_str = '''sql2 -qmap.dev.query.akadns.net "select name, count(1) counts, string_join(cast(asnum as string),',') aslist from a_maprule_qos_aslist group by name;" | sed 's/\s\+/,/g' | tail -n+3 | head -n-1 > %s''' % config.aslist_file
    sp.check_call(query_str, shell=True)

    check_str = '''wc -l %s | awk '{print $1}' ''' % config.aslist_file
    n_lines = int(sp.check_output(check_str, shell=True).rstrip('\n'))
    print "initial query takes time: %s" % str(time.time()-tic)

    maximum_retrial = 20
    n_retrials = 1

    while n_lines < 10:
        if n_retrials < maximum_retrial:
            tic = time.time()
            sp.check_call(query_str, shell=True)
            n_lines = int(sp.check_output(check_str, shell=True).rstrip('\n'))
            print "retrial #%s query takes time: %s" % (n_retrials, str(time.time()-tic))
        else:
            print "reached maximum retrials."
            break



if __name__ == '__main__':
    sys.exit(main())