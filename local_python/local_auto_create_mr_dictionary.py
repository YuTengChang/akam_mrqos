#!/opt/anaconda/bin/python
"""
Created on Thu March 16 12:47:15 2016

@author: ychang
"""
import sys, os
import shutil
import time
import calendar
import pandas as pd
import numpy
import subprocess as sp
sys.path.append('/home/ychang/Documents/Projects/18-DDC/MRQOS/')
import configurations.config as config

def main():
    table_local_dir = '/u4/ychang/Projects/18-MRQOS/Data/'
    table_local_file = 'mr_auto_dictionary.txt'
    formatting = ' --csv '
    sql_cmd = ''' select mapruleid, service, replace(substr(description,1,25),',',';') description, mrname + ' [' + cast(mapruleid as string) + ']' mrn, priority from (select mapruleid, substr(name, 3, instr(name,'.')-3) mrname, case service when 'W' then 'FreeFlow' when 'S' then 'ESSL' else 'Other' end service,  priority, description from mcm_maprules); '''
    sp_cmd = '''/usr/bin/sql2 -qmap.dev.query.akadns.net %s "%s" | tail -n+3  > %s ''' % (formatting,
                                                                                          sql_cmd,
                                                                                          os.path.join(table_local_dir, table_local_file) )
    sp.check_call(sp_cmd, shell=True)

    # sync with old file (mr_dictionary)
    filename = 'mr_dictionary.txt'
    file_source =  os.path.join(table_local_dir, filename)
    data = numpy.genfromtxt(file_source, delimiter=',', skip_header=0, dtype='str')
    headers = ["maprule1", "service1", "description1", "mname1", "priority1"]
    df = pd.DataFrame(data, columns=headers)
    file_source =  os.path.join(table_local_dir, table_local_file)
    data = numpy.genfromtxt(file_source, delimiter=',', skip_header=0, dtype='str')
    headers = ["maprule", "service", "description", "mname", "priority"]
    df2 = pd.DataFrame(data, columns=headers)

    df.index = df.maprule1
    df2.index = df2.maprule
    df_all = pd.concat([df2,df], axis=1, join_axes=[df2.index])
    df_all.fillna(0, inplace=True)
    df_all['description'] = [x if y==0 else y for (x,y) in zip(df_all.description, df_all.description1)]
    df_all.drop(['maprule1', 'service1', 'description1', 'mname1', 'priority1'], axis=1, inplace=True)

    df_all.to_csv(os.path.join(table_local_dir, 'mr_final_dictionary.txt'), header=False, index=False)

if __name__ == '__main__':
    sys.exit(main())