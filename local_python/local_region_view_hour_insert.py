#!/opt/anaconda/bin/python
"""
Created on Thu March 09 12:47:15 2016

@author: ychang
"""
import sys, os
#import shutil
import time
import calendar
import subprocess as sp

def main():
    ts = calendar.timegm(time.gmtime())
    print "###################"
    print "# Performing the hourly region_view_hour data fetch and insert"
    print "# starting processing time is " + str(ts)
    print "###################"



if __name__ == '__main__':
    sys.exit(main())
