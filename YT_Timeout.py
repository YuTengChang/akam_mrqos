# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 09:45:06 2015

@author: ychang
"""

import signal
import time
 
def test_request(arg=None):
    """Your http request."""
    time.sleep(2)
    return arg

def test_function():
    flag = 0
    count = 0
    n_retrial = 10
    while (flag == 0) and (count < n_retrial):
        try:
            with Timeout(1):
                time.sleep(4-count)
                flag = 1
        except:
            count += 1
            print "count = %s" % str(count)


class Timeout():
    """Timeout class using ALARM signal."""
    class Timeout(Exception):
        pass
 
    def __init__(self, sec):
        self.sec = sec
 
    def __enter__(self):
        signal.signal(signal.SIGALRM, self.raise_timeout)
        signal.alarm(self.sec)
 
    def __exit__(self, *args):
        signal.alarm(0)    # disable alarm
 
    def raise_timeout(self, *args):
        raise Timeout.Timeout()