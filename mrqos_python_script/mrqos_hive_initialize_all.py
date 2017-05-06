#!//usr/bin/python
"""
Created on Thu May 06 08:17:35 2017

@author: ychang
"""
import sys, os

sys.path.append('/home/testgrp/MRQOS/')
import configurations.config as config
import configurations.beeline as beeline
import glob

def main():
    root = config.mrqos_root
    hive_init_query_loc = os.path.join(root, 'mrqos_hive_init')
    hive_init_list = glob.glob(os.path.join(hive_init_query_loc, '*.hive'))

    for hive_init in hive_init_list:
        print "now initializing HIVE table from file %s".format(hive_init)
        beeline.bln_f(hive_init)


if __name__ == '__main__':
    sys.exit(main())