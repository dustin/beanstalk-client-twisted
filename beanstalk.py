from twisted.protocols import basic
from twisted.internet import defer, protocol
from twisted.python import log
from StringIO import StringIO

# Stolen from memcached protocol
try:
    from collections import deque
except ImportError:
    class deque(list):
        def popleft(self):
            return self.pop(0)

class Command(object):
    """
    Wrap a client action into an object, that holds the values used in the
    protocol.

    @ivar _deferred: the L{Deferred} object that will be fired when the result
        arrives.
    @type _deferred: L{Deferred}

    @ivar command: name of the command sent to the server.
    @type command: C{str}
    """

    def __init__(self, command, **kwargs):
        """
        Create a command.

        @param command: the name of the command.
        @type command: C{str}

        @param kwargs: this values will be stored as attributes of the object
            for future use
        """
        self.command = command
        self._deferred = defer.Deferred()
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return "<Command: %s>" % self.command

    def success(self, value):
        """
        Shortcut method to fire the underlying deferred.
        """
        self._deferred.callback(value)


    def fail(self, error):
        """
        Make the underlying deferred fails.
        """
        self._deferred.errback(error)

class Beanstalk(basic.LineReceiver):

    def __init__(self):
        self._current = deque()
        self._lenExpected = None
        self._getBuffer = None
        self._bufferLength = None

    def rawDataRecevied(self, data):
        self.current_command=None

    def connectionMade(self):
        print "Connected!"
        self.setLineMode()

    def stats(self, arg=None):
        if arg:
            cmd="stats " + arg
        else:
            cmd="stats"
        self.sendLine(cmd)
        cmdObj = Command('stats')
        self._current.append(cmdObj)
        return cmdObj._deferred

    def use(self, tube):
        self.sendLine("use %s" % tube)
        cmdObj = Command('use', tube=tube)
        self._current.append(cmdObj)
        return cmdObj._deferred

    def watch(self, tube):
        self.sendLine("watch %s" % tube)
        cmdObj = Command('watch', tube=tube)
        self._current.append(cmdObj)
        return cmdObj._deferred

    def cmd_USING(self, line):
        cmd = self._current.popleft()
        cmd.success(line)

    def cmd_WATCHING(self, line):
        cmd = self._current.popleft()
        cmd.success(int(line))

    def cmd_OK(self, line):
        cmd = self._current[0]
        length = line
        self._lenExpected = int(length)
        self._getBuffer = []
        self._bufferLength = 0
        cmd.length = self._lenExpected
        self.setRawMode()

    def lineReceived(self, line):
        """
        Receive line commands from the server.
        """
        token = line.split(" ", 1)[0]
        # First manage standard commands without space
        cmd = getattr(self, "cmd_%s" % (token,), None)
        if cmd is not None:
            args = line.split(" ", 1)[1:]
            if args:
                cmd(args[0])
            else:
                cmd()
        else:
            print "Unknown response", `line`

    def parseStats(self, v):
        lines=v.strip().split("\n")[1:]
        return dict([l.split(": ") for l in lines])

    def rawDataReceived(self, data):
        self._getBuffer.append(data)
        self._bufferLength += len(data)
        if self._bufferLength >= self._lenExpected + 2:
            data = "".join(self._getBuffer)
            buf = data[:self._lenExpected]
            rem = data[self._lenExpected + 2:]
            val = buf
            self._lenExpected = None
            self._getBuffer = None
            self._bufferLength = None
            cmd = self._current[0]
            cmd.value = val
            x = self._current.popleft()
            if cmd.command == "stats":
                cmd.success(self.parseStats(cmd.value))

            self.setLineMode(rem)

class BeanstalkClientFactory(protocol.ClientFactory):
    def startedConnecting(self, connector):
        print 'Started to connect.'

    def buildProtocol(self, addr):
        print 'Connected.'
        return Beanstalk()

    def clientConnectionLost(self, connector, reason):
        print 'Lost connection.  Reason:', reason

    def clientConnectionFailed(self, connector, reason):
        print 'Connection failed. Reason:', reason