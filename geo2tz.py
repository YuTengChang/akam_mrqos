# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 14:03:11 2015

@author: ychang
"""

from datetime import datetime
from pytz import timezone
import pytz
import numpy;
import pandas as pd;

fmt = '%z'
output = [];

data = numpy.genfromtxt('/u4/ychang/Projects/18-MRQOS/s236b_backup/geolist.txt',delimiter=' ', dtype='str');

for item in range(len(data)):
    try:
        tmp = timezone(str(pytz.country_timezones(data[item])[0]))
        s1 = tmp.localize(datetime(2015,4,20,12,0,0), is_dst=False).strftime(fmt)[0:3]
        output.append( data[item] + " " + s1[0] + str(int(s1[1:3])) );
    except:
        output.append( data[item] )
    
for queue in output:
    print queue
    
    