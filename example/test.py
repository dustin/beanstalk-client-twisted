#!/usr/bin/env python

import os
import sys
sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol, defer

import beanstalk

testDeferreds=[]
failures=[]

d=protocol.ClientCreator(reactor, beanstalk.Beanstalk).connectTCP(
    sys.argv[1], 11300)

def print_cb(name):
    def f(val):
        print name, `val`
    return f

def success_print(v):
    sys.stdout.write(".")
    sys.stdout.flush()

def handle_failure(cmd):
    def f(v):
        sys.stdout.write("E")
        sys.stdout.flush()
        failures.append((cmd, v))
    return f

def handle_unexpected_success(cmd):
    def f(v):
        # XXX:  Why would I get callback on error?
        if v is None:
            sys.stdout.write(".")
        else:
            sys.stdout.write("E")
            failures.append((cmd, v))
        sys.stdout.flush()
    return f

def success(f, *args):
    rv = f(*args).addCallback(success_print).addErrback(
        handle_failure(f.__name__))
    testDeferreds.append(rv)
    return rv

def failure(f, *args):
    rv = f(*args).addErrback(success_print).addCallback(
        handle_unexpected_success(f.__name__))
    testDeferreds.append(rv)
    return rv

def listChecker(cmd, expected):
    def f(v):
        hasAll = True
        for i in expected:
            if not i in v:
                hasAll = False
        if hasAll:
            sys.stdout.write(".")
        else:
            sys.stdout.write("E")
            failures.append((cmd, "missing an expected element in " + `v`))
    rv=cmd().addCallback(f)
    return rv

def runCommands(bs):
    success(bs.stats)
    success(bs.use, "crack")
    success(bs.watch, "tv")
    success(bs.ignore, "tv")
    success(bs.watch, "crack")
    success(bs.put, 8192, 0, 300, 'This is a job')
    def runJob(j):
        id, job=j
        success_print(None)
        return success(bs.delete, id)
    bs.reserve(1).addCallback(runJob)
    failure(bs.reserve, 1)
    listChecker(bs.list_tubes, ['default', 'crack'])

    dl=defer.DeferredList(testDeferreds)
    def done(v):
        sys.stdout.write("\n")
        print "Finished all tests."
        if failures:
            print "Failures:"
            for f in failures:
                print "\t" + `f`
        reactor.stop()
    dl.addCallback(done)

d.addCallback(runCommands)

reactor.run()
