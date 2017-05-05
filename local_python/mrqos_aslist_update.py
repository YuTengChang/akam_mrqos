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
    query_str = '''sql2 -qmap.dev.query.akadns.net "select name, count(1) counts, string_join(cast(asnum as string),':') aslist from a_maprule_qos_aslist group by name;" | sed 's/\s\+/,/g' | tail -n+3 | head -n-2 | sed 's/^,//g' > %s''' % config.aslist_file
    sp.check_call(query_str, shell=True)

    check_str = '''wc -l %s | awk '{print $1}' ''' % config.aslist_file
    n_lines = int(sp.check_output(check_str, shell=True).rstrip('\n'))
    print "initial query takes time: %s sec and lines: %s." % (str(time.time()-tic), str(n_lines))

    maximum_retrial = 20
    n_retrials = 1
    success_fetch_flag = True

    while n_lines < 10:
        if n_retrials < maximum_retrial:
            tic = time.time()
            sp.check_call(query_str, shell=True)
            n_lines = int(sp.check_output(check_str, shell=True).rstrip('\n'))
            print "retrial #%s query takes time: %s sec and lines: %s." % (n_retrials, str(time.time()-tic), str(n_lines))
        else:
            print "reached maximum retrials."
            success_fetch_flag = False
            break

    # initialize and import the SQLite table
    if success_fetch_flag:
        import_str = ''' /opt/anaconda/bin/sqlite3 %s < %s ''' % (config.case_view_hour_db, config.alist_init_import)
        sp.check_call(import_str, shell=True)

    # update the file for Hive data updates
    cmd = """cat {} | awk -F, '{s=split($3,a,":"); for (i=1; i<=s; i++){print a[i]","$1}}' > {}""".format(config.aslist_file,
                                                                                                          config.aslist_hive_file)
    sp.check_call(cmd, shell=True)

if __name__ == '__main__':
    sys.exit(main())