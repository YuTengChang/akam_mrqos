#!/opt/anaconda/bin/python
"""
Created on Thu March 16 12:47:15 2016

@author: ychang
"""
import sys, os
import subprocess as sp

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)
import configurations.config as config


def main():
    db_location = '/opt/web-data/SQLite3/pp_coverage.db'
    print "initialize the table for pp converage data."
    if os.path.isfile(db_location):
        os.remove(db_location)

    cmd_str = '/opt/anaconda/bin/sqlite3 %s < %s' % (db_location,
                                                     config.pp_coverage_init)

    print "the new db lives in: %s" % db_location
    sp.check_call(cmd_str, shell=True)


if __name__ == '__main__':
    sys.exit(main())