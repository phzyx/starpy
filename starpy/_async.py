"""Self-contained asyncio compatibility shim for the starpy fork.

starpy historically imported Twisted::

    from twisted.internet import protocol, reactor, defer
    from twisted.protocols import basic
    from twisted.internet import error as tw_error

The Twisted removal (design doc doc/untwist/, Section 4) replaces those imports
with this module, which reproduces the small Twisted surface starpy actually
drives -- Deferred/maybeDeferred, a reconnecting client factory, a plain server
factory, a LineOnlyReceiver, an asyncio-backed reactor (connectTCP/callLater),
and the ConnectionDone error type -- as thin wrappers over ``asyncio``.

Why a *separate* shim rather than importing ``asterisk.aio``:

  * starpy ships as its own installable package (``pip install starpy``) and must
    not gain a runtime dependency on the Asterisk test suite. This module is
    therefore self-contained: it imports only the standard library.

Cross-shim interoperability with ``asterisk.aio`` (they meet whenever the suite
drives starpy) is preserved by two conventions copied verbatim from that layer:

  * a Failure marks itself with the class attribute ``_is_failure = True`` so the
    other shim's callback chains route a foreign Failure to the errback branch
    without importing one concrete class;
  * a Deferred exposes ``addBoth`` so the other shim can pause its chain on ours.

Both shims obtain the event loop via ``asyncio.get_running_loop()`` first, so when
starpy is embedded in the running suite they share the one loop. The reactor here
also schedules network binds immediately whenever *any* loop is running -- even if
this module's own ``reactor.run()`` was never called -- so starpy works while the
suite (not starpy) owns the loop.

This module mirrors the structure and semantics of ``asterisk.aio`` (defer.py,
failure.py, reactor.py, protocols.py); see doc/untwist/02-design.md for the
rationale behind the Future-wrapper Deferred and resource-owned shutdown.
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
    """Twisted-style Deferred backed by an explicit callback chain.

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
# Protocol base classes (mirror of twisted.internet.protocol / protocols.basic)
# ============================================================================ #
class Protocol(object):
    """Minimal twisted.internet.protocol.Protocol surface.

    ``makeConnection`` stores the transport and calls ``connectionMade``; the
    reactor adapter drives ``dataReceived`` / ``connectionLost``.
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
    """Twisted's basic.LineOnlyReceiver ported to the asyncio adapter.

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
        """Discard and return any buffered partial line (Twisted parity)."""
        b, self._buffer = self._buffer, b''
        return b


class Factory(object):
    """twisted.internet.protocol.Factory: builds a protocol per connection."""

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
    """twisted.internet.protocol.ClientFactory: adds connection callbacks."""

    def clientConnectionFailed(self, connector, reason):
        """Called when a connection attempt fails (override as needed)."""

    def clientConnectionLost(self, connector, reason):
        """Called when an established connection is lost (override as needed)."""


class ReconnectingClientFactory(ClientFactory):
    """twisted.internet.protocol.ReconnectingClientFactory.

    Reconnects with exponential backoff after a lost/failed connection. starpy's
    AMIFactory subclasses this and calls ``resetDelay()`` on a good connection and
    ``retry(connector)`` on loss.
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
        self._callID = reactor.callLater(self.delay, connector.connect)

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
# Reactor (mirror of asterisk.aio.reactor, self-contained)
# ============================================================================ #
class ReactorNotRunning(Exception):
    """stop() called when the reactor is not running (twisted parity)."""


class ReactorAlreadyRunning(Exception):
    """run() called when the reactor is already running (twisted parity)."""


class AlreadyCalled(Exception):
    """_DelayedCall.cancel() when the call already fired."""


class AlreadyCancelled(Exception):
    """_DelayedCall.cancel() when already cancelled."""


class _DelayedCall(object):
    """Cancellable scheduled call (twisted.internet.base.DelayedCall)."""

    def __init__(self, reactor, delay, fn, args, kw):
        self._reactor = reactor
        self._delay = delay
        self._fn = fn
        self._args = args
        self._kw = kw
        self._cancelled = False
        self._called = False
        self._handle = reactor._loop.call_later(delay, self._fire)

    def _fire(self):
        self._called = True
        self._reactor._delayed_calls.discard(self)
        self._fn(*self._args, **self._kw)

    def active(self):
        return not (self._cancelled or self._called)

    def cancel(self):
        if self._called:
            raise AlreadyCalled()
        if self._cancelled:
            raise AlreadyCancelled()
        self._cancelled = True
        self._handle.cancel()
        self._reactor._delayed_calls.discard(self)

    def reset(self, delay):
        if not self.active():
            raise AlreadyCalled()
        self._handle.cancel()
        self._delay = delay
        self._handle = self._reactor._loop.call_later(delay, self._fire)

    def delay(self, seconds_later):
        self.reset(self._delay + seconds_later)


class _Port(object):
    """Handle for a listening TCP endpoint (twisted IListeningPort)."""

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
    """Handle for an outgoing TCP connection (twisted IConnector).

    Supports reconnection: on an established-then-lost connection the adapter
    notifies the factory via ``clientConnectionLost``; the factory's ``retry``
    calls ``connect()`` again. Honors ``timeout`` and ``bindAddress`` per attempt.
    """

    def __init__(self, reactor, host, port, factory, timeout, bindAddress):
        self._reactor = reactor
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
        loop = self._reactor._ensure_loop()
        local_addr = self._bindAddress if self._bindAddress else None
        coro = loop.create_connection(
            lambda: _TwistedProtocolAdapter(factory, self),
            self.host, self.port, local_addr=local_addr)
        if self._timeout:
            coro = asyncio.wait_for(coro, self._timeout)

        def apply(result):
            transport, _proto = result
            self._transport = transport

        def on_error(exc):
            if self._stopped:
                return
            if hasattr(factory, 'clientConnectionFailed'):
                factory.clientConnectionFailed(self, Failure(exc))

        self._reactor._register_bind(
            coro, apply, on_error,
            'connectTCP:%s:%d' % (self.host, self.port))

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


class _TwistedProtocolAdapter(asyncio.Protocol):
    """Drive a Twisted-style protocol from asyncio.Protocol callbacks."""

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


class _TCPTransportAdapter(object):
    """Expose the twisted ITransport surface used by protocols over TCP."""

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


class _Reactor(object):
    """asyncio-backed stand-in for twisted.internet.reactor (starpy subset)."""

    def __init__(self):
        self.running = False
        self._loop = None
        self._when_running = []
        self._pending_binds = []
        self._stop_future = None
        self._failure = None
        self._delayed_calls = set()
        self._tasks = set()
        self._ports = []
        self._connectors = []

    def _ensure_loop(self):
        # Prefer the running loop so an embedded starpy shares the suite's loop.
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            if self._loop is None:
                self._loop = asyncio.get_event_loop_policy().get_event_loop()
        return self._loop

    # -- lifecycle -------------------------------------------------------- #
    def run(self, installSignalHandlers=True):
        if self.running:
            raise ReactorAlreadyRunning()
        loop = self._ensure_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
        self.running = True
        self._failure = None
        self._stop_future = loop.create_future()

        pending = self._pending_binds
        self._pending_binds = []
        for coro, apply, on_error, label in pending:
            try:
                result = loop.run_until_complete(coro)
            except Exception as exc:
                if on_error is not None:
                    on_error(exc)
                    continue
                self.running = False
                self._failure = exc
                loop.run_until_complete(self._shutdown())
                self._stop_future = None
                raise
            apply(result)

        queued = self._when_running
        self._when_running = []
        for fn, args, kw in queued:
            loop.call_soon(fn, *args, **kw)

        try:
            loop.run_until_complete(self._stop_future)
        finally:
            self.running = False
            loop.run_until_complete(self._shutdown())
            self._stop_future = None

        if self._failure is not None:
            failure = self._failure
            self._failure = None
            raise failure

    def stop(self):
        if not self.running:
            return
        self.running = False
        fut = self._stop_future

        def _resolve():
            if fut is not None and not fut.done():
                fut.set_result(None)

        self._loop.call_soon_threadsafe(_resolve)

    async def _shutdown(self):
        for dc in list(self._delayed_calls):
            if dc.active():
                dc.cancel()
        self._delayed_calls.clear()

        for port in list(self._ports):
            port.stopListening()
        self._ports.clear()
        for connector in list(self._connectors):
            connector.disconnect()
        self._connectors.clear()

        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        try:
            current = asyncio.current_task(self._loop)
            stragglers = [t for t in asyncio.all_tasks(self._loop)
                          if t is not current and not t.done()]
        except RuntimeError:
            stragglers = []
        for task in stragglers:
            task.cancel()
        if stragglers:
            await asyncio.gather(*stragglers, return_exceptions=True)

        await asyncio.sleep(0)

    # -- scheduling ------------------------------------------------------- #
    def callWhenRunning(self, fn, *args, **kw):
        if self.running:
            self._ensure_loop().call_soon(fn, *args, **kw)
        else:
            self._when_running.append((fn, args, kw))

    def callLater(self, delay, fn, *args, **kw):
        self._ensure_loop()
        dc = _DelayedCall(self, delay, fn, args, kw)
        self._delayed_calls.add(dc)
        return dc

    def callFromThread(self, fn, *args, **kw):
        self._ensure_loop().call_soon_threadsafe(lambda: fn(*args, **kw))

    def callInThread(self, fn, *args, **kw):
        loop = self._ensure_loop()
        deferred = Deferred()
        fut = loop.run_in_executor(None, lambda: fn(*args, **kw))
        self._tasks.add(fut)

        def _done(f):
            self._tasks.discard(f)
            try:
                deferred.callback(f.result())
            except Exception:
                deferred.errback(Failure())

        fut.add_done_callback(_done)
        return deferred

    # -- networking ------------------------------------------------------- #
    def listenTCP(self, port, factory, backlog=50, interface=''):
        loop = self._ensure_loop()
        handle = _Port()
        self._ports.append(handle)
        coro = loop.create_server(lambda: _TwistedProtocolAdapter(factory),
                                  interface or '0.0.0.0', port,
                                  backlog=backlog)

        def apply(server):
            handle._set_server(server)

        self._register_bind(coro, apply, None, 'listenTCP:%d' % port)
        return handle

    def connectTCP(self, host, port, factory, timeout=30, bindAddress=None):
        self._ensure_loop()
        connector = _Connector(self, host, port, factory, timeout, bindAddress)
        self._connectors.append(connector)
        connector.connect()
        return connector

    # -- internal bind scheduling ----------------------------------------- #
    def _register_bind(self, coro, apply, on_error, label):
        """Bind now if a loop is running (even one this reactor doesn't own),
        else queue for this reactor's own run() startup.

        Scheduling on any running loop is what lets starpy operate while the
        Asterisk suite -- not starpy -- owns the event loop.
        """
        try:
            asyncio.get_running_loop()
            loop_running = True
        except RuntimeError:
            loop_running = False

        if self.running or loop_running:
            task = asyncio.ensure_future(coro)
            self._tasks.add(task)

            def _done(t):
                self._tasks.discard(t)
                if t.cancelled():
                    return
                exc = t.exception()
                if exc is not None:
                    if on_error is not None:
                        on_error(exc)
                    else:
                        self._fatal(exc)
                    return
                apply(t.result())

            task.add_done_callback(_done)
        else:
            self._pending_binds.append((coro, apply, on_error, label))

    def _fatal(self, exc):
        if self._failure is None:
            self._failure = exc
        if self.running:
            try:
                self.stop()
            except ReactorNotRunning:
                pass


# Module-level singleton, mirroring ``from twisted.internet import reactor``.
reactor = _Reactor()


# ============================================================================ #
# Twisted-shaped import namespaces
# ============================================================================ #
# starpy replaces
#   from twisted.internet import protocol, reactor, defer
#   from twisted.protocols import basic
#   from twisted.internet import error as tw_error
# with
#   from starpy._async import protocol, reactor, defer, basic
#   from starpy._async import error as tw_error
# These SimpleNamespace objects reproduce the referenced attributes so the module
# bodies (defer.Deferred, protocol.ReconnectingClientFactory, basic.LineOnlyReceiver,
# tw_error.ConnectionDone, ...) resolve unchanged.
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
