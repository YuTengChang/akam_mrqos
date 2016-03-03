#!/home/ychang/anaconda/bin/python2.7
"""
Created on Thu Apr 16 16:47:15 2015

@author: ychang
"""
import sys, os
import shutil
import subprocess as sp
import time


def main():
    """
    getting the content a_maprule_qos_view_by_region table from mapmon and clean up (10 rows per case) and then upload
    to the cluster
    :return:
    """

    # current time
    timenow = int(time.time())

    print "###################"
    print "Obtaining the a_maprule_qos_view_by_region content"
    print "starting processing time is " + str(timenow)
    print "###################"

    print "    ****  obtaining from mapmon."
    cmd_str = """ gwsh -2 $mapmon_machine "/a/bin/sql2 --csv ' select * from _local_a_maprule_qos_view_by_region ' > ~/full-table-mrqos-view-by-region" """
    sp.check_call(cmd_str, shell=True)



if __name__ == '__main__':
    sys.exit(main())
