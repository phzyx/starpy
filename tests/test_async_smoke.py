#!/usr/bin/env python3
"""Isolated smoke test for the Twisted-free starpy fork (design doc Section 4.4).

Exercises the two protocols starpy ships -- the AMI client (manager.py) and the
FastAGI server (fastagi.py) -- end to end over real loopback TCP, driven only by
``starpy._async`` and the standard library. It imports NO Twisted and NONE of the
Asterisk test suite, proving starpy stands on its own after the cutover.

Coverage (matches the exit criterion in 03-implementation.md Section 4.4: AMI
runs an action, FastAGI completes a simple dialog, reconnect parity holds):

  * AMI action -- AMIFactory.login() connects, the protocol auto-logs-in, then we
    issue a real ``ping`` action and assert the Success response comes back.
  * FastAGI dialog -- reactor.listenTCP() serves a FastAGIFactory; a stub Asterisk
    client sends the AGI variable block, the handler issues ``ANSWER`` and we
    assert the ``200 result=0`` reply is parsed to the integer 0.
  * AMI reconnect -- the stub manager drops the connection right after login; we
    assert the ReconnectingClientFactory retries, re-logs-in, and fires
    on_reconnect with a fresh protocol (the riskiest starpy behavior).
  * AMI disconnect cleanup -- multiple pending actions are all notified even
    when each callback removes itself from the callback dictionary.

Run:  python3 tests/test_async_smoke.py      (exit 0 on success)
"""

import asyncio
import os
import sys

# Import starpy from the repo without installation.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starpy import manager, fastagi
from starpy._async import reactor

# Fail loudly if anything dragged Twisted in.
assert 'twisted' not in sys.modules, "Twisted must not be importable via starpy"


# --------------------------------------------------------------------------- #
# Stub Asterisk manager (AMI) server
# --------------------------------------------------------------------------- #
def _make_ami_server(drop_first_after_login=False):
    """Return a connection handler that answers AMI actions with Success.

    ``drop_first_after_login`` closes the *first* connection immediately after
    answering its login, so the client observes a lost connection and must
    reconnect (exercises ReconnectingClientFactory).
    """
    state = {'conns': 0}

    async def handler(reader, writer):
        state['conns'] += 1
        my_index = state['conns']
        try:
            while True:
                headers = {}
                try:
                    while True:
                        raw = await reader.readuntil(b'\r\n')
                        line = raw[:-2].decode('utf-8')
                        if line == '':
                            break
                        key, value = line.split(':', 1)
                        headers[key.strip().lower()] = value.strip()
                except (asyncio.IncompleteReadError, ConnectionError):
                    return
                if not headers:
                    return
                action = headers.get('action', '').lower()
                actionid = headers.get('actionid', '')
                lines = ['response: Success', 'actionid: %s' % actionid]
                if action == 'login':
                    lines.append('message: Authentication accepted')
                elif action == 'ping':
                    lines.append('ping: Pong')
                writer.write(('\r\n'.join(lines) + '\r\n\r\n').encode('utf-8'))
                await writer.drain()
                if drop_first_after_login and my_index == 1 and action == 'login':
                    return  # close this connection -> client reconnects
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            writer.close()

    return handler


# --------------------------------------------------------------------------- #
# Part A: AMI login + a real action (ping)
# --------------------------------------------------------------------------- #
async def _run_ami_action():
    server = await asyncio.start_server(
        _make_ami_server(), '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    proto = None
    try:
        factory = manager.AMIFactory('user', 'secret')
        proto = await asyncio.wait_for(
            factory.login('127.0.0.1', port, timeout=5), timeout=5)
        assert isinstance(proto, manager.AMIProtocol), \
            "login result is not an AMIProtocol: %r" % (proto,)
        assert proto.factory is factory, "protocol not linked to its factory"

        # Issue a real AMI action over the same connection.
        response = await asyncio.wait_for(proto.ping(), timeout=5)
        assert isinstance(response, dict), "ping result not a message: %r" % (
            response,)
        assert response.get('response') == 'Success', \
            "ping did not succeed: %r" % (response,)

        factory.stopTrying()
        proto.transport.loseConnection()
        await asyncio.sleep(0.05)
    finally:
        server.close()
        await server.wait_closed()
    print("  [AMI action]  login + ping OK (response=%s)"
          % response.get('response'))


# --------------------------------------------------------------------------- #
# Part B: FastAGI server completes a command dialog
# --------------------------------------------------------------------------- #
async def _run_fastagi_dialog():
    loop = asyncio.get_event_loop()
    got = loop.create_future()

    def main_function(proto):
        # Called once the AGI variable block has been read: run one command.
        d = proto.answer()

        def done(result):
            if not got.done():
                got.set_result((dict(proto.variables), result))
            proto.transport.loseConnection()
            return result

        d.addBoth(done)

    port_handle = reactor.listenTCP(0, fastagi.FastAGIFactory(main_function))
    for _ in range(200):
        if port_handle._server is not None:
            break
        await asyncio.sleep(0.01)
    assert port_handle._server is not None, "FastAGI server never bound"
    port = port_handle._server.sockets[0].getsockname()[1]

    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    try:
        agi_vars = [
            b'agi_network: yes',
            b'agi_request: agi://localhost',
            b'agi_channel: SIP/mike-0001',
            b'agi_uniqueid: 1139871605.0',
            b'agi_extension: 1',
            b'',                       # blank line terminates the block
        ]
        writer.write(b'\n'.join(agi_vars) + b'\n')
        await writer.drain()

        # The handler issues ANSWER; play Asterisk and reply with a result.
        command = await asyncio.wait_for(reader.readuntil(b'\n'), timeout=5)
        assert command.strip() == b'ANSWER', \
            "unexpected FastAGI command: %r" % (command,)
        writer.write(b'200 result=0\n')
        await writer.drain()

        variables, result = await asyncio.wait_for(got, timeout=5)
        assert variables.get('agi_channel') == 'SIP/mike-0001', \
            "agi_channel mis-parsed: %r" % (variables.get('agi_channel'),)
        assert result == 0, "ANSWER result not parsed to 0: %r" % (result,)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionError, OSError):
            pass
        port_handle.stopListening()
        await asyncio.sleep(0.05)
    print("  [FastAGI]     variable block + ANSWER dialog OK "
          "(%d vars, result=%s)" % (len(variables), result))


# --------------------------------------------------------------------------- #
# Part C: AMI disconnect -> automatic reconnect + re-login
# --------------------------------------------------------------------------- #
async def _run_ami_reconnect():
    server = await asyncio.start_server(
        _make_ami_server(drop_first_after_login=True), '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]

    loop = asyncio.get_event_loop()
    reconnected = loop.create_future()

    def on_reconnect(new_login):
        def fired(proto):
            if not reconnected.done():
                reconnected.set_result(proto)
            return proto
        new_login.addCallback(fired)

    proto2 = None
    try:
        factory = manager.AMIFactory('user', 'secret', on_reconnect=on_reconnect)
        # Fast, deterministic backoff for the test.
        factory.initialDelay = 0.05
        factory.factor = 1.0
        factory.jitter = 0
        factory.maxDelay = 0.05

        proto1 = await asyncio.wait_for(
            factory.login('127.0.0.1', port, timeout=5), timeout=5)
        assert isinstance(proto1, manager.AMIProtocol)

        # Server dropped the first connection after login; the factory should
        # retry, re-login, and fire on_reconnect with a brand-new protocol.
        proto2 = await asyncio.wait_for(reconnected, timeout=8)
        assert isinstance(proto2, manager.AMIProtocol), \
            "reconnect did not yield an AMIProtocol: %r" % (proto2,)
        assert proto2 is not proto1, "reconnect reused the old protocol object"

        factory.stopTrying()
        if proto2.transport is not None:
            proto2.transport.loseConnection()
        await asyncio.sleep(0.05)
    finally:
        server.close()
        await server.wait_closed()
    print("  [AMI reconnect] dropped -> auto reconnect + re-login OK")


# --------------------------------------------------------------------------- #
# Part D: disconnect notifies every pending action without dict mutation errors
# --------------------------------------------------------------------------- #
def _run_ami_disconnect_cleanup():
    proto = manager.AMIProtocol()
    notified = []

    def pending(action_id):
        def callback(reason):
            notified.append(action_id)
            # This mirrors sendDeferred's cleanup callback: handling the
            # connection-lost result removes the action from the same mapping
            # connectionLost is traversing.
            proto.actionIDCallbacks.pop(action_id, None)
        return callback

    proto.actionIDCallbacks = {
        'one': pending('one'),
        'two': pending('two'),
    }
    proto.eventTypeCallbacks = {'Event': [object()]}
    proto.connectionLost(None)

    assert notified == ['one', 'two'], \
        "not every pending AMI action was notified: %r" % notified
    assert proto.actionIDCallbacks == {}, "pending AMI actions not cleared"
    assert proto.eventTypeCallbacks == {}, "AMI event callbacks not cleared"
    print("  [AMI cleanup]  all pending actions notified during disconnect OK")


async def _main():
    print("starpy Twisted-free smoke test")
    _run_ami_disconnect_cleanup()
    await _run_ami_action()
    await _run_fastagi_dialog()
    await _run_ami_reconnect()
    print("ALL OK")


if __name__ == '__main__':
    # Overall cap so a regression surfaces as a failure, not a wall-clock hang.
    asyncio.run(asyncio.wait_for(_main(), timeout=30))
