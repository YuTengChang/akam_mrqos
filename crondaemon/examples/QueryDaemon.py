#!/a/bin/python2.7
import sys,os
import subprocess as sp
import multiprocessing
import logging
sys.path.append("/home/testgrp/perfTMI/thruway")
import etc.tmi_lib as tmi 
import datetime
import time
from threading import Thread

'''
input_args = {
    'input_table' : <input.table>,
    'output_table': <output.table>,
    'partition_var':<partition.var>,
    'query_file' : <query.file>
    'log':<log.file>
    'backstop': <backstop partition>,
    'exclusion_list': <list of partitions to exclude>
    }
'''

class queryDaemon(multiprocessing.Process):
    
    def __init__(self,args,backstop=0,exclusion_list=[]):
        multiprocessing.Process.__init__(self)
        self.args = args
        self.log = logging.getLogger(self.args['log'])
        self.max_in = None
        self.backstop = backstop
        self.exclusion_list = exclusion_list
        
    def run(self):
        #implements run method of multiprocesing.Process
        #do not change
        
        kill_count = 0 
        while True:
            if kill_count == 4: self.terminate() 
            self.log.info("Getting missing partitions.")
            try:
                pm = self.partitions_missing()
            except Exception:
                self.log.exception("partitions_missing() failed with error:")
                self.log.error("Aborting this process.")
                sys.exit()
            self.log.info("The following partitions are missing:")
            pm = [p for p in pm if self.clear_to_proceed(p)]
            if len(pm) == 0:
                self.log.info("No partitions are missing.")
            else:
                for p in pm:
                    self.log.info("Partition value %s",p)
                try:
                    self.log.info("Now processing partition %s",pm[-1])
                    self.run_query(pm[-1])
                    kill_count = 0 
                except Exception:
                    self.log.exception("Unable to run query on partition %s",
                                        pm[-1])
                    kill_count += 1
            self.log.info("Going to sleep.")
            time.sleep(self.args['sleep_interval'])
                    
        
    def partitions_missing(self):
        
        partitions_in = tmi.get_existing_partitions(self.args['input_table'])
        partitions_out = tmi.get_existing_partitions(self.args['output_table'])
        partitions_miss = [i for i in partitions_in if i not in partitions_out]
        if partitions_miss == []:
            return []
        else:
            partitions_miss.sort()
            self.max_in = tmi.startdate_to_datetime(repr(max(partitions_in)))
            return partitions_miss
    
    def clear_to_proceed(self,partition_val):
        
        td = datetime.timedelta(hours=self.args['offset'])
        if tmi.startdate_to_datetime(repr(partition_val)) <= self.max_in - td:
            if partition_val >= self.backstop and partition_val not in self.exclusion_list:
                return True
            else:
                return False
        else:
            return False

    def hour_offset(self,stdate):
        #convert startdate to date time
        stdate_dte = tmi.datetime_to_startdate(stdate)
        offset = datetime.timedelta(hours = self.args['offset'])
        return stdate_dte - offset 
        
    
    def run_query(self,part_val):
        #this gets overwritten 
        return 
    
    
    
    
        