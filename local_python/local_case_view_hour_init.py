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

    print "initialize the table for case_view_hour data."

    os.remove(config.case_view_hour_db)

    cmd_str = '/opt/anaconda/bin/sqlite3 %s < %s' % (config.case_view_hour_db,
                                                     config.case_view_hour_init)

    print "the new db lives in: %s" % config.case_view_hour_db
    sp.check_call(cmd_str, shell=True)


if __name__ == '__main__':
    sys.exit(main())