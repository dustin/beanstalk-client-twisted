#!/usr/bin/env python

import os
import sys
sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol, defer, task

import beanstalk

failures=[]

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
    return f(*args).addCallback(success_print).addErrback(
        handle_failure(f.__name__))

def failure(f, *args):
    return f(*args).addErrback(success_print).addCallback(
        handle_unexpected_success(f.__name__))

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
        sys.stdout.flush()
    rv=cmd().addCallback(f)
    return rv

def valueChecker(cmd, expected, *args):
    def f(v):
        if v == expected:
            sys.stdout.write(".")
        else:
            sys.stdout.write("E")
            failures.append((cmd, "expected %s == %s" % (`v`, `expected`)))
        sys.stdout.flush()
    rv=cmd(*args).addCallback(f)
    return rv

torun = []

def run(f, *args, **kwargs):
    torun.append((f, args, kwargs))

def runGenerator():
    for (f, args, kwargs) in torun:
        d=f(*args)
        if 'callback' in kwargs:
            d.addCallback(kwargs['callback'])
        if 'errback' in kwargs:
            d.addErrback(kwargs['errback'])
        yield d

def jobStats(bs, id):
    def f(x):
        return bs.stats_job(id).addCallback(success_print).addErrback(
            handle_failure("job_stats"))
    return f

def jobCallbacks(bs):
    def f(stuff):
        inserted, i=stuff
        return bs.peek(i).addCallback(success_print).addErrback(
            handle_failure("peek")).addBoth(jobStats(bs, i))
    return f

def runCommands(bs):
    run(success, bs.stats)
    run(success, bs.use, "crack")
    run(success, bs.stats_tube, "crack")
    run(success, bs.watch, "tv")
    run(success, bs.ignore, "tv")
    run(success, bs.watch, "crack")
    run(bs.put, 8192, 0, 300, 'This is a job', callback=jobCallbacks(bs))
    run(success, bs.peek_ready)
    def releaseJob(j):
        id, job=j
        success_print(None)
        return success(bs.release, id, 1024, 0)
    run(bs.reserve, 1, callback=releaseJob)
    def buryJob(j):
        id, job=j
        success_print(None)
        return success(bs.bury, id, 1024)
    run(bs.reserve, 1, callback=buryJob, errback=print_cb("r-n-b error"))
    run(success, bs.peek_buried)
    run(bs.kick, 1)
    def runJob(j):
        id, job=j
        success_print(None)
        return success(bs.delete, id)
    run(bs.reserve, 1, callback=runJob)
    run(failure, bs.reserve, 1)
    run(failure, bs.peek_delayed)
    run(listChecker, bs.list_tubes, ['default', 'crack'])
    run(listChecker, bs.list_tubes_watched, ['default', 'crack'])
    run(valueChecker, bs.used_tube, 'crack')

    def done(v):
        sys.stdout.write("\n")
        print "Finished all tests."
        if failures:
            print "Failures:"
            for f in failures:
                print "\t" + `f`
        reactor.stop()

    coop = task.Cooperator()
    doneDeferred=defer.Deferred()
    doneDeferred.addCallback(done)
    coop.coiterate(runGenerator(), doneDeferred=doneDeferred)

#
# The weekend starts here.
#
d=protocol.ClientCreator(reactor, beanstalk.Beanstalk).connectTCP(
    sys.argv[1], 11300)
d.addCallback(runCommands)

reactor.run()
