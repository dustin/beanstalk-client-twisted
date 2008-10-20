#!/usr/bin/env python

import os
import sys
sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol

import beanstalk

d=protocol.ClientCreator(reactor, beanstalk.Beanstalk).connectTCP(
    sys.argv[1], 11300)

def runCommands(bs):
    def p(name):
        def f(val):
            print name, `val`
        return f
    bs.stats().addCallback(p("stats"))
    bs.use("crack").addCallback(p("crack"))
    bs.watch("tv").addCallback(p("watch"))
    bs.ignore("tv").addCallback(p("ignore"))
    bs.watch('crack')
    bs.put(8192, 0, 300, 'This is a job').addCallback(p("put"))
    def runJob(j):
        id, job=j
        print "Running job:  ", j
        bs.delete(id).addCallback(p("deleted"))
    bs.reserve(1).addCallback(runJob)
    bs.reserve(1).addErrback(p("failing reserve"))

d.addCallback(runCommands)

reactor.run()
