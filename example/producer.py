#!/usr/bin/env python

import os
import sys
sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol, defer, task

import beanstalk

def worker(bs):
    bs.use("myqueue")
    bs.put(8192, 0, 300, 'Look!  A job!').addCallback(
        lambda x: sys.stdout.write("Queued job: %s\n" % `x`)).addCallback(
        lambda x: reactor.stop())

d=protocol.ClientCreator(reactor, beanstalk.Beanstalk).connectTCP(
    sys.argv[1], 11300)
d.addCallback(worker)

reactor.run()

