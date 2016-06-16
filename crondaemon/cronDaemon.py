#!/a/bin/python2.7
import sys,os
import subprocess as sp
import multiprocessing
import logging
import datetime
import time



class cronDaemon(multiprocessing.Process):

    def __init__(self,args):
        multiprocessing.Process.__init__(self)
        self.args = args
        self.log = logging.getLogger(self.args['log'])


    def run(self):
        # implements run method of multiprocesing.Process
        # do not change the name of this method

        while True:

            self.log.info("Running the thing we want to run.")
            try:
                self.run_job()
            except sp.CalledProcessError:
                self.log.exception("Unable to execute command.")

            except Exception:
                self.log.exception("Job failed with exception.")
                self.log.error("Aborting")
                sys.exit()

            time.sleep(self.args['sleep_interval'])


    def run_job(self):

        # this gets overwritten when you define your own class
        sp.check_call("echo abcd",shell=True)

        return
