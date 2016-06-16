import sys,os
import backgroundStart as bs
import cronDaemon as cd


if __name__=='__main__':

    args = { 'log_file': './log',
             'log': 'theLog',
             'sleep_interval': 10,}

    dmn = cd.cronDaemon(args)
    proc = bs.backgroundStarter(args,'./pid.st',dmn)

    proc.run()




