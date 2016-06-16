import sys
sys.path.append('..')
import cronDaemon
import subprocess as sp



class exampleDaemon(cronDaemon.cronDaemon):


    def __init__(self,args):
        cronDaemon.cronDaemon.__init__(self,args)
        self.log.info("starting Example")



    def run_job(self):

        res = sp.check_output("gwsh s187m.ddc.akamai.com '/a/third-party/hadoop/bin/hadoop fs -ls %s'"%args['ls_d'],
                        shell=True)

        print res






