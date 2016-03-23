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
    query_str = '''sql2 -qmap.dev.query.akadns.net "select name, count(1) counts, string_join(cast(asnum as string),',') aslist from a_maprule_qos_aslist group by name;" | awk 'NF==3{print $1, $2, $3}' | tail -n-3 | head -n-1 > %s''' % config.aslist_file
    sp.check_call(query_str, shell=True)


if __name__ == '__main__':
    sys.exit(main())