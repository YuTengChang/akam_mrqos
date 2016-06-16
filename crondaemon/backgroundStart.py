import sys,os
import logging
import errno


'''
oopsi_args = {
'input_table':'as_ecor_hourly',
'output_table':'oopsi',
'partition_var':'startdate',
'sleep_interval': 10,
'log' :'oopsi.log',
'log_file':'/home/testgrp/perfTMI/logs/oopsi.log',
'offset':0
}
'''


class backgroundStarter():
    
    def __init__(self,args,pid_dir,daemon):

        self.args = args
        self.pid_dir = pid_dir
        self.daemon = daemon
        self.start_logging() 
        
        
    def start_logging(self):
        logging.basicConfig(
            filename = self.args['log_file'],
            format = "%(levelname) -10s %(asctime)s %(message)s",
            level = logging.INFO)
        
        self.log = logging.getLogger(self.args['log'])
        self.log.info("Intiating logging.")
    
    def run(self):
        
        self.log.info("This is the cron script.")
        self.pid = self.get_info()
        if self.pid == -1:
            self.log.info("Starting a new daemon")
            self.daemon.start()
            self.pid = self.daemon.pid
            self.save_info()
        
        else:
            self.log.info("See you in an hour")
        
    
    def save_info(self):
        self.log.info("Recording pid %s as running",self.pid)
        with open(self.pid_dir,'w') as f:
            f.write(repr(self.pid))
    
    
    def get_info(self):
        
        self.log.info("Checking to see if process is already running")
        try:
            with open(self.pid_dir,'r') as f:
                pid = int(f.read().strip())
        except IOError:
            self.log.info("No running process found.")
            return -1
        if pid_exists(pid):
            self.log.info("Process is already running with pid %s",pid)
            return pid
        else:
            self.log.info("Process is not running.")
            return -1
            
    
def pid_exists(pid):
    '''Check whether pid exists in the current process table.
        UNIX only.
    '''
    if pid < 0:
        return False
    if pid == 0:
        # According to "man 2 kill" PID 0 refers to every process
        # in the process group of the calling process.
        # On certain systems 0 is a valid PID but we have no way
        # to know that in a portable fashion.
        raise ValueError('invalid PID 0')
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            # ESRCH == No such process
            return False
        elif err.errno == errno.EPERM:
            # EPERM clearly means there's a process to deny access to
            return True
        else:
            # According to "man 2 kill" possible error values are
            # (EINVAL, EPERM, ESRCH)
            raise
    else:
        return True