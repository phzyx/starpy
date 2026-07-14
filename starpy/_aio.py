"""Native asyncio primitives for the Twisted-free starpy fork.

This module supersedes the ``starpy._async`` compatibility shim. It keeps the
two pieces of that shim starpy genuinely relies on -- the chainable *and*
awaitable ``Deferred``/``Failure`` result type, and the ``LineOnlyReceiver``
line-framing protocol base -- and drops the emulated Twisted ``reactor``
entirely. Connecting, listening and delayed calls run directly on the caller's
*running* event loop via the standard library:

  * ``connect_tcp(host, port, factory, ...)``  -> loop.create_connection
  * ``listen_tcp(port, factory, ...)``         -> loop.create_server
  * ``call_later(delay, fn, *args)``           -> loop.call_later

Why keep a starpy-local ``Deferred`` rather than plain coroutines: the Asterisk
test suite drives starpy through ~180 fire-and-forget ``.addCallback`` /
``.addErrback`` chains (e.g. ``ami.originate(...).addErrback(handler)``) as well
as ``await`` sites. A chainable+awaitable Deferred satisfies both without any
churn in the test corpus. It is pure standard library, so the no-Twisted gate
(which bans only ``twisted``/``txaio``/``autobahn`` imports) passes.

Cross-shim interoperability with ``asterisk.aio`` (they meet whenever the suite
drives starpy) is preserved by two conventions:

  * a Failure marks itself with ``_is_failure = True`` so a foreign Failure is
    routed to the errback branch without importing a concrete class;
  * a Deferred exposes ``addBoth`` so the other layer can pause its chain on ours.

Everything obtains the loop via ``asyncio.get_running_loop()`` first, so an
embedded starpy shares the suite's single loop.
"""

import asyncio
import inspect
import types


# ============================================================================ #
# Failure (mirror of asterisk.aio.failure.Failure)
# ============================================================================ #
class Failure(object):
    """Wraps an exception for transport through Deferred errback chains."""

    # Cross-shim marker: recognised by asterisk.aio's _is_failure duck-test.
    _is_failure = True

    def __init__(self, exc=None, exc_type=None, tb=None):
        import sys
        if exc is None:
            etype, evalue, etb = sys.exc_info()
            if evalue is None:
                evalue = Exception("Unknown failure (no active exception)")
                etype = type(evalue)
                etb = None
            exc = evalue
            exc_type = exc_type or etype
            tb = tb if tb is not None else etb
        self.value = exc
        self.type = exc_type or type(exc)
        self.tb = tb

    def getErrorMessage(self):
        return str(self.value)

    def check(self, *error_types):
        for et in error_types:
            if isinstance(self.value, et):
                return et
            try:
                if issubclass(self.type, et):
                    return et
            except TypeError:
                pass
        return None

    def trap(self, *error_types):
        et = self.check(*error_types)
        if et is None:
            self.raiseException()
        return et

    def raiseException(self):
        if isinstance(self.value, BaseException):
            raise self.value.with_traceback(self.tb)
        raise RuntimeError(str(self.value))

    def getTraceback(self):
        import traceback as _traceback
        if self.tb is not None:
            return ''.join(
                _traceback.format_exception(self.type, self.value, self.tb))
        return str(self.value)

    def __repr__(self):
        return "<Failure %s: %s>" % (
            getattr(self.type, '__name__', self.type), self.value)


# ============================================================================ #
# Deferred (mirror of asterisk.aio.defer)
# ============================================================================ #
class AlreadyCalledError(Exception):
    """callback()/errback() invoked on an already-fired Deferred."""


class TimeoutError(Exception):
    """twisted.internet.defer.TimeoutError."""


def _passthrough(result):
    return result


def _get_loop():
    """Return the running loop if any, else the current policy loop."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.get_event_loop_policy().get_event_loop()


def _is_failure(obj):
    """True if ``obj`` is a Failure from this or any compatible shim."""
    return getattr(obj, '_is_failure', False) is True


def _is_deferred_like(obj):
    """True if ``obj`` quacks like a Deferred (has an ``addBoth`` method)."""
    return callable(getattr(obj, 'addBoth', None))


class Deferred(object):
    """Chainable + awaitable result, backed by an explicit callback chain.

    Wraps mutable chain state (rather than subclassing asyncio.Future) so that a
    callback added after firing still threads the current result, and exposes
    ``__await__`` over an idle event so ``await d`` observes the latest result.
    """

    def __init__(self, canceller=None):
        self._chain = []
        self._called = False
        self._chain_result = None
        self._running = False
        self._paused = 0
        self._canceller = canceller
        self._idle_event = None

    @property
    def called(self):
        return self._called

    @property
    def result(self):
        return self._chain_result

    # -- callback registration ------------------------------------------- #
    def addCallbacks(self, callback, errback=None,
                     callbackArgs=None, callbackKeywords=None,
                     errbackArgs=None, errbackKeywords=None):
        cb = (callback, tuple(callbackArgs or ()), dict(callbackKeywords or {}))
        if errback is None:
            eb = None
        else:
            eb = (errback, tuple(errbackArgs or ()), dict(errbackKeywords or {}))
        self._chain.append((cb, eb))
        if self._called:
            self._run_callbacks()
        return self

    def addCallback(self, callback, *args, **kw):
        return self.addCallbacks(callback,
                                 callbackArgs=args, callbackKeywords=kw)

    def addErrback(self, errback, *args, **kw):
        return self.addCallbacks(_passthrough, errback,
                                 errbackArgs=args, errbackKeywords=kw)

    def addBoth(self, fn, *args, **kw):
        return self.addCallbacks(fn, fn,
                                 callbackArgs=args, callbackKeywords=kw,
                                 errbackArgs=args, errbackKeywords=kw)

    def chainDeferred(self, other):
        return self.addCallbacks(other.callback, other.errback)

    # -- firing ----------------------------------------------------------- #
    def callback(self, result=None):
        self._start(result)

    def errback(self, fail=None):
        if fail is None:
            fail = Failure()
        elif isinstance(fail, BaseException):
            fail = Failure(fail)
        elif not _is_failure(fail):
            fail = Failure(RuntimeError(str(fail)))
        self._start(fail)

    def _start(self, result):
        if self._called:
            raise AlreadyCalledError()
        self._called = True
        self._chain_result = result
        self._run_callbacks()

    # -- chain execution -------------------------------------------------- #
    def _run_callbacks(self):
        if self._running or self._paused:
            return
        self._running = True
        self._clear_idle()
        try:
            while self._chain:
                cb, eb = self._chain.pop(0)
                stage = eb if _is_failure(self._chain_result) else cb
                if stage is None:
                    continue
                fn, args, kw = stage
                if fn is _passthrough:
                    continue
                try:
                    new_result = fn(self._chain_result, *args, **kw)
                except Exception:
                    self._chain_result = Failure()
                    continue
                if _is_deferred_like(new_result):
                    self._pause_on_deferred(new_result)
                    return
                if isinstance(new_result, asyncio.Future) or \
                        inspect.isawaitable(new_result):
                    self._pause_on_future(new_result)
                    return
                self._chain_result = new_result
        finally:
            self._running = False
        self._settle()

    def _pause_on_deferred(self, inner):
        self._paused += 1
        self._chain_result = None

        def _resume(res):
            self._chain_result = res
            self._paused -= 1
            self._run_callbacks()
            return res

        inner.addBoth(_resume)

    def _pause_on_future(self, awaitable):
        self._paused += 1
        self._chain_result = None
        task = asyncio.ensure_future(awaitable)

        def _resume(fut):
            self._paused -= 1
            if fut.cancelled():
                self._chain_result = Failure(asyncio.CancelledError())
            elif fut.exception() is not None:
                self._chain_result = Failure(fut.exception())
            else:
                self._chain_result = fut.result()
            self._run_callbacks()

        task.add_done_callback(_resume)

    # -- await support ---------------------------------------------------- #
    def _get_idle_event(self):
        if self._idle_event is None:
            self._idle_event = asyncio.Event()
        return self._idle_event

    def _clear_idle(self):
        if self._idle_event is not None:
            self._idle_event.clear()

    def _settle(self):
        if self._called and not self._running and not self._paused:
            self._get_idle_event().set()

    def __await__(self):
        return self._await_result().__await__()

    async def _await_result(self):
        while not (self._called and not self._running and not self._paused):
            await self._get_idle_event().wait()
            self._get_idle_event().clear()
        if _is_failure(self._chain_result):
            self._chain_result.raiseException()
        return self._chain_result

    # -- cancellation ----------------------------------------------------- #
    def cancel(self, msg=None):
        if not self._called:
            if self._canceller is not None:
                try:
                    self._canceller(self)
                except Exception:
                    pass
            if not self._called:
                self.errback(Failure(asyncio.CancelledError()))
            return True
        return False


def succeed(result):
    d = Deferred()
    d.callback(result)
    return d


def fail(failure=None):
    d = Deferred()
    d.errback(failure)
    return d


def DeferredList(deferreds, fireOnOneCallback=False, fireOnOneErrback=False,
                 consumeErrors=False):
    deferreds = list(deferreds)
    result_list = [None] * len(deferreds)
    state = {'remaining': len(deferreds), 'fired': False}
    dlist = Deferred()

    if not deferreds:
        dlist.callback(result_list)
        return dlist

    def _record(result, index, succeeded):
        result_list[index] = (succeeded, result)
        state['remaining'] -= 1
        if not state['fired']:
            if succeeded and fireOnOneCallback:
                state['fired'] = True
                dlist.callback((result, index))
            elif (not succeeded) and fireOnOneErrback:
                state['fired'] = True
                dlist.errback(result)
            elif state['remaining'] == 0:
                state['fired'] = True
                dlist.callback(result_list)
        if (not succeeded) and consumeErrors:
            return None
        return result

    for index, d in enumerate(deferreds):
        d.addCallbacks(_record, _record,
                       callbackArgs=(index, True),
                       errbackArgs=(index, False))
    return dlist


def gatherResults(deferreds, consumeErrors=False):
    dl = DeferredList(deferreds, fireOnOneErrback=True,
                      consumeErrors=consumeErrors)

    def _strip(results):
        return [r for (_success, r) in results]

    dl.addCallbacks(_strip, _passthrough)
    return dl


def _from_awaitable(awaitable):
    """Adapt a Future/Task/coroutine into a Deferred that fires on completion."""
    d = Deferred()
    task = asyncio.ensure_future(awaitable)

    def _done(fut):
        if fut.cancelled():
            d.errback(Failure(asyncio.CancelledError()))
        elif fut.exception() is not None:
            exc = fut.exception()
            try:
                raise exc
            except Exception:
                d.errback(Failure())
        else:
            d.callback(fut.result())

    task.add_done_callback(_done)
    return d


def maybeDeferred(f, *args, **kw):
    """Invoke f; wrap a plain return in a fired Deferred, pass a Deferred through.

    A coroutine or Future returned by ``f`` is scheduled and adapted into a
    Deferred (never left un-awaited)."""
    try:
        result = f(*args, **kw)
    except Exception:
        return fail(Failure())
    if isinstance(result, Deferred) or _is_deferred_like(result):
        return result
    if isinstance(result, asyncio.Future) or inspect.isawaitable(result):
        return _from_awaitable(result)
    if _is_failure(result):
        return fail(result)
    return succeed(result)


# ============================================================================ #
# Errors (mirror of twisted.internet.error subset)
# ============================================================================ #
class ConnectionDone(Exception):
    """Connection closed cleanly (twisted.internet.error.ConnectionDone)."""


class ConnectionLost(Exception):
    """Connection lost unexpectedly (twisted.internet.error.ConnectionLost)."""


# ============================================================================ #
# Protocol base classes (line framing, native)
# ============================================================================ #
class Protocol(object):
    """Minimal protocol surface driven by the transport adapter.

    ``makeConnection`` stores the transport and calls ``connectionMade``; the
    adapter drives ``dataReceived`` / ``connectionLost``.
    """

    transport = None
    factory = None
    connected = 0

    def makeConnection(self, transport):
        self.connected = 1
        self.transport = transport
        self.connectionMade()

    def connectionMade(self):
        """Called when a connection is established (override as needed)."""

    def dataReceived(self, data):
        """Called with incoming bytes (override as needed)."""

    def connectionLost(self, reason):
        """Called when the connection is lost (override as needed)."""


class LineOnlyReceiver(Protocol):
    """Line-oriented protocol base (Twisted basic.LineOnlyReceiver semantics).

    Splits the incoming byte stream on ``delimiter`` and calls
    ``lineReceived(line)`` for each complete line; ``sendLine(line)`` appends the
    delimiter and writes to the transport. A line longer than ``MAX_LENGTH``
    triggers ``lineLengthExceeded`` (default: drop the connection).
    """

    _buffer = b''
    delimiter = b'\r\n'
    MAX_LENGTH = 16384

    def dataReceived(self, data):
        """Buffer bytes and dispatch each completed line."""
        lines = (self._buffer + data).split(self.delimiter)
        # The final element is the (possibly empty) trailing partial line.
        self._buffer = lines.pop(-1)
        for line in lines:
            if self.transport is None:
                # A lineReceived handler dropped the connection; stop.
                return
            if len(line) > self.MAX_LENGTH:
                return self.lineLengthExceeded(line)
            self.lineReceived(line)
        if len(self._buffer) > self.MAX_LENGTH:
            partial, self._buffer = self._buffer, b''
            return self.lineLengthExceeded(partial)

    def lineReceived(self, line):
        """Override: called with each complete line (delimiter stripped)."""

    def sendLine(self, line):
        """Send ``line`` followed by the delimiter."""
        if isinstance(line, str):
            line = line.encode('utf-8')
        return self.transport.write(line + self.delimiter)

    def lineLengthExceeded(self, line):
        """Called when a line exceeds MAX_LENGTH (default: drop connection)."""
        if self.transport is not None:
            self.transport.loseConnection()

    def clearLineBuffer(self):
        """Discard and return any buffered partial line."""
        b, self._buffer = self._buffer, b''
        return b


class Factory(object):
    """Builds a protocol instance per connection."""

    protocol = None

    def buildProtocol(self, addr):
        """Instantiate ``self.protocol`` and back-link the factory."""
        p = self.protocol()
        p.factory = self
        return p

    def doStart(self):
        """Called when the factory starts (override as needed)."""

    def doStop(self):
        """Called when the factory stops (override as needed)."""

    def startedConnecting(self, connector):
        """Called when a connection attempt begins (override as needed)."""


class ClientFactory(Factory):
    """Adds client connection lifecycle callbacks."""

    def clientConnectionFailed(self, connector, reason):
        """Called when a connection attempt fails (override as needed)."""

    def clientConnectionLost(self, connector, reason):
        """Called when an established connection is lost (override as needed)."""


class ReconnectingClientFactory(ClientFactory):
    """Reconnects with exponential backoff after a lost/failed connection.

    starpy's AMIFactory subclasses this and calls ``resetDelay()`` on a good
    connection and ``retry(connector)`` on loss. The reconnect timer runs on the
    caller's running loop via ``call_later`` (no reactor).
    """

    maxDelay = 3600
    initialDelay = 1.0
    factor = 2.7182818284590451      # (math.e)
    jitter = 0.11962656472
    delay = initialDelay
    retries = 0
    maxRetries = None
    _callID = None
    connector = None
    continueTrying = 1

    def clientConnectionFailed(self, connector, reason):
        if self.continueTrying:
            self.connector = connector
            self.retry(connector)

    def clientConnectionLost(self, connector, unused_reason):
        if self.continueTrying:
            self.connector = connector
            self.retry(connector)

    def retry(self, connector=None):
        """Schedule a reconnect attempt after the current backoff delay."""
        if not self.continueTrying:
            return
        if connector is None:
            connector = self.connector
            if connector is None:
                raise ValueError("no connector to retry")

        self.retries += 1
        if self.maxRetries is not None and (self.retries > self.maxRetries):
            return

        self.delay = min(self.delay * self.factor, self.maxDelay)
        if self.jitter:
            import random
            self.delay = random.normalvariate(self.delay,
                                               self.delay * self.jitter)
        self._callID = call_later(self.delay, connector.connect)

    def stopTrying(self):
        """Abandon reconnection attempts."""
        if self._callID is not None:
            try:
                self._callID.cancel()
            except Exception:
                pass
            self._callID = None
        self.continueTrying = 0

    def resetDelay(self):
        """Reset backoff state after a successful connection."""
        self.delay = self.initialDelay
        self.retries = 0
        self._callID = None
        self.continueTrying = 1


# ============================================================================ #
# Transport adapters (native asyncio.Protocol <-> starpy Protocol surface)
# ============================================================================ #
class _TCPTransportAdapter(object):
    """Expose the small transport surface starpy protocols use over TCP."""

    def __init__(self, transport):
        self._transport = transport

    def write(self, data):
        self._transport.write(data)

    def writeSequence(self, seq):
        self._transport.writelines(seq)

    def loseConnection(self):
        self._transport.close()

    def getPeer(self):
        return self._transport.get_extra_info('peername')

    def getHost(self):
        return self._transport.get_extra_info('sockname')

    def __getattr__(self, name):
        return getattr(self._transport, name)


class _ProtocolAdapter(asyncio.Protocol):
    """Drive a starpy Protocol from asyncio.Protocol callbacks.

    The wrapped protocol comes from ``factory.buildProtocol(addr)`` and exposes
    ``makeConnection``/``dataReceived``/``connectionLost``. For *client*
    connections (connect_tcp) an established-then-lost connection notifies the
    factory so a ReconnectingClientFactory can retry; server connections
    (listen_tcp) have no connector and are not given clientConnectionLost.
    """

    def __init__(self, factory, connector=None):
        self._factory = factory
        self._connector = connector
        self._proto = None

    def connection_made(self, transport):
        peer = transport.get_extra_info('peername')
        self._proto = self._factory.buildProtocol(peer)
        adapter = _TCPTransportAdapter(transport)
        if hasattr(self._proto, 'makeConnection'):
            self._proto.makeConnection(adapter)
        else:
            self._proto.transport = adapter
            if hasattr(self._proto, 'connectionMade'):
                self._proto.connectionMade()

    def data_received(self, data):
        if self._proto is not None:
            self._proto.dataReceived(data)

    def connection_lost(self, exc):
        reason = Failure(exc) if exc is not None else Failure(ConnectionDone())
        if self._proto is not None and hasattr(self._proto, 'connectionLost'):
            self._proto.connectionLost(reason)
        connector = self._connector
        if connector is not None:
            connector._transport = None
            if not connector._stopped and \
                    hasattr(self._factory, 'clientConnectionLost'):
                self._factory.clientConnectionLost(connector, reason)


# ============================================================================ #
# Connect / listen / delayed-call primitives (running-loop, no reactor)
# ============================================================================ #
class _Port(object):
    """Handle for a listening TCP endpoint."""

    def __init__(self):
        self._transport = None
        self._server = None
        self._closed = False

    def _set_server(self, server):
        if self._closed and server is not None:
            server.close()
            return
        self._server = server

    def stopListening(self):
        self._closed = True
        if self._transport is not None:
            self._transport.close()
            self._transport = None
        if self._server is not None:
            self._server.close()
            self._server = None


class _Connector(object):
    """Handle for an outgoing TCP connection with reconnect support.

    On an established-then-lost connection the adapter notifies the factory via
    ``clientConnectionLost``; the factory's ``retry`` calls ``connect()`` again.
    Honors ``timeout`` and ``bindAddress`` per attempt. Runs on the caller's
    running loop.
    """

    def __init__(self, host, port, factory, timeout, bindAddress):
        self.host = host
        self.port = port
        self._factory = factory
        self._timeout = timeout
        self._bindAddress = bindAddress
        self._transport = None
        self._stopped = False

    def connect(self):
        if self._stopped:
            return
        self._transport = None
        factory = self._factory
        if hasattr(factory, 'startedConnecting'):
            factory.startedConnecting(self)
        loop = _get_loop()
        local_addr = self._bindAddress if self._bindAddress else None
        coro = loop.create_connection(
            lambda: _ProtocolAdapter(factory, self),
            self.host, self.port, local_addr=local_addr)
        if self._timeout:
            coro = asyncio.wait_for(coro, self._timeout)

        task = asyncio.ensure_future(coro)

        def _done(t):
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                if self._stopped:
                    return
                if hasattr(factory, 'clientConnectionFailed'):
                    factory.clientConnectionFailed(self, Failure(exc))
                return
            transport, _proto = t.result()
            self._transport = transport

        task.add_done_callback(_done)

    def stopConnecting(self):
        self.disconnect()

    def disconnect(self):
        self._stopped = True
        factory = self._factory
        if hasattr(factory, 'stopTrying'):
            factory.stopTrying()
        if self._transport is not None:
            self._transport.close()
            self._transport = None


def connect_tcp(host, port, factory, timeout=30, bindAddress=None):
    """Open a client TCP connection, building a protocol from ``factory``.

    Returns a connector supporting reconnection through a
    ReconnectingClientFactory. Runs on the caller's running loop.
    """
    connector = _Connector(host, port, factory, timeout, bindAddress)
    connector.connect()
    return connector


def listen_tcp(port, factory, backlog=50, interface=''):
    """Listen for TCP connections, building protocols from ``factory``.

    Returns a ``_Port`` handle whose ``_server`` is set once the bind completes.
    Runs on the caller's running loop.
    """
    loop = _get_loop()
    handle = _Port()
    coro = loop.create_server(lambda: _ProtocolAdapter(factory),
                              interface or '0.0.0.0', port, backlog=backlog)
    task = asyncio.ensure_future(coro)

    def _done(t):
        if t.cancelled():
            return
        if t.exception() is not None:
            raise t.exception()
        handle._set_server(t.result())

    task.add_done_callback(_done)
    return handle


def call_later(delay, fn, *args, **kw):
    """Schedule ``fn(*args, **kw)`` after ``delay`` seconds on the running loop.

    Returns the asyncio ``TimerHandle`` (has a ``.cancel()`` method), so callers
    that hold the handle can cancel a pending call.
    """
    loop = _get_loop()
    if kw:
        return loop.call_later(delay, lambda: fn(*args, **kw))
    return loop.call_later(delay, fn, *args)


# ============================================================================ #
# Import namespaces (drop-in for the retired starpy._async namespaces)
# ============================================================================ #
defer = types.SimpleNamespace(
    Deferred=Deferred,
    DeferredList=DeferredList,
    gatherResults=gatherResults,
    maybeDeferred=maybeDeferred,
    succeed=succeed,
    fail=fail,
    TimeoutError=TimeoutError,
    AlreadyCalledError=AlreadyCalledError,
)

protocol = types.SimpleNamespace(
    Protocol=Protocol,
    Factory=Factory,
    ClientFactory=ClientFactory,
    ReconnectingClientFactory=ReconnectingClientFactory,
)

basic = types.SimpleNamespace(
    LineOnlyReceiver=LineOnlyReceiver,
    Protocol=Protocol,
)

error = types.SimpleNamespace(
    ConnectionDone=ConnectionDone,
    ConnectionLost=ConnectionLost,
)
