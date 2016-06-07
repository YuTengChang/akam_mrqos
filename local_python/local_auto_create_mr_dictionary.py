#!/opt/anaconda/bin/python
"""
Created on Thu March 16 12:47:15 2016

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
    table_local_dir = '/u4/ychang/Projects/18-MRQOS/Data/'
    table_local_file = 'mr_auto_dictionary.txt'
    sql_cmd = ''' select mapruleid, service, description, mrname + ' [' + cast(mapruleid as string) + ']' mrn, priority from (select mapruleid, substr(name, 3, instr(name,'.')-3) mrname, case service when 'W' then 'FreeFlow' when 'S' then 'ESSL' else 'Other' end service,  priority, description from mcm_maprules limit 5); '''
    sp_cmd = '''/usr/bin/sql2 -qmap.dev.query.akadns.net "%s" | tail -n+3 | head -n-2 > %s ''' % ( sql_cmd,
                                                                                                   os.path.join(table_local_dir, table_local_file) )
    sp.check_call(sp_cmd, shell=True)

if __name__ == '__main__':
    sys.exit(main())