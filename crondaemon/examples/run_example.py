import exampleDaemon as ed
import backgroundStart as bs


if __name__=='__main__':


    args = { 'log_file': './log',
             'log': 'theLog',
             'sleep_interval': 10,
             'ls_d': '/ghostcache/hadoop/data/perfTMI'}

    dmn = ed.exampleDaemon(args)
    proc = bs.backgroundStarter(args,'./pid.st',dmn)

    proc.run()

