# -*- coding: utf-8 -*-

__version__ = "1.0.7"

import asyncio
import socket
import errno
import msgpack
import base64

import tarantool
from tarantool.response import Response
from tarantool.request import (
    Request,
    RequestCall,
    RequestDelete,
    RequestEval,
    RequestInsert,
    RequestJoin,
    RequestReplace,
    RequestPing,
    RequestSelect,
    RequestSubscribe,
    RequestUpdate,
    RequestAuthenticate)

from tarantool.schema import SchemaIndex, SchemaSpace
from tarantool.error import SchemaError
import tarantool.const
from tarantool.utils import check_key

from tarantool.error import (
    NetworkError,
    DatabaseError,
    warn,
    RetryWarning)

from tarantool.const import (
    REQUEST_TYPE_OK,
    REQUEST_TYPE_ERROR,
    RETRY_MAX_ATTEMPTS,
    IPROTO_GREETING_SIZE,
    ENCODING_DEFAULT)


import logging

logger = logging.getLogger(__package__)


def connect(host, post, user=None, password=None, loop=None, encoding=ENCODING_DEFAULT):
    conn = Connection(host, post, user=user, password=password, loop=loop, encoding=encoding)

    return conn


class Schema(object):
    def __init__(self, con):
        self.schema = {}
        self.con = con

    @asyncio.coroutine
    def get_space(self, space):
        try:
            return self.schema[space]
        except KeyError:
            pass

        if not self.con.connected:
            yield from self.con.connect()

        with (yield from self.con.lock):
            if space in self.schema:
                return self.schema[space]

            _index = (tarantool.const.INDEX_SPACE_NAME
                      if isinstance(space, str)
                      else tarantool.const.INDEX_SPACE_PRIMARY)

            array = yield from self.con.select(tarantool.const.SPACE_SPACE, space, index=_index)
            if len(array) > 1:
                raise SchemaError('Some strange output from server: \n' + array)

            if len(array) == 0 or not len(array[0]):
                temp_name = ('name' if isinstance(space, str) else 'id')
                raise SchemaError(
                    "There's no space with {1} '{0}'".format(space, temp_name))

            array = array[0]
            return SchemaSpace(array, self.schema)

    @asyncio.coroutine
    def get_index(self, space, index):
        _space = yield from self.get_space(space)
        try:
            return _space.indexes[index]
        except KeyError:
            pass

        if not self.con.connected:
            yield from self.con.connect()

        with (yield from self.con.lock):
            if index in _space.indexes:
                return _space.indexes[index]

            _index = (tarantool.const.INDEX_INDEX_NAME
                      if isinstance(index, str)
                      else tarantool.const.INDEX_INDEX_PRIMARY)

            array = yield from self.con.select(tarantool.const.SPACE_INDEX, [_space.sid, index], index=_index)

            if len(array) > 1:
                raise SchemaError('Some strange output from server: \n' + array)

            if len(array) == 0 or not len(array[0]):
                temp_name = ('name' if isinstance(index, str) else 'id')
                raise SchemaError(
                    "There's no index with {2} '{0}' in space '{1}'".format(
                        index, _space.name, temp_name))

            array = array[0]
            return SchemaIndex(array, _space)

    def flush(self):
        self.schema.clear()


class Connection(tarantool.Connection):
    DatabaseError = DatabaseError

    def __init__(self, host, port, user=None, password=None, connect_now=False, loop=None,
                 encoding=ENCODING_DEFAULT, aiobuffer_size=16384):
        """just create instance, do not really connect by default"""

        super().__init__(host, port,
                         user=user,
                         password=password,
                         connect_now=connect_now,
                         encoding=encoding)

        self.aiobuffer_size = aiobuffer_size
        assert isinstance(self.aiobuffer_size, int)

        self.loop = loop or asyncio.get_event_loop()
        self.lock = asyncio.Semaphore(loop=self.loop)
        self._reader = None
        self._writer = None

        self.connect_now = connect_now
        self.connected = False
        self.req_num = 0

        self._waiters = dict()
        self._reader_task = None
        self._writer_task = None
        self._write_event = None
        self._write_buf = None
        self._auth_event = None
        self._salt = None

        self.error = False  # important not raise exception in response reader
        self.schema = Schema(self)  # need schema with lock

    @asyncio.coroutine
    def connect(self):
        if self.connected:
            return

        with (yield from self.lock):
            if self.connected:
                return

            logger.log(logging.DEBUG, "connecting to %r" % self)
            self._reader, self._writer = yield from asyncio.open_connection(self.host, self.port, loop=self.loop)
            self.connected = True

            self._reader_task = self.loop.create_task(self._response_reader())
            self._writer_task = self.loop.create_task(self._response_writer())
            self._write_event = asyncio.Event(loop=self.loop)
            self._write_buf = b""

            self._auth_event = asyncio.Event(loop=self.loop)

        if self.user and self.password:
            yield from self._auth_event.wait()
            yield from self.authenticate(self.user, self.password)

    @asyncio.coroutine
    def _response_writer(self):
        while True:
            yield from self._write_event.wait()

            if self._write_buf:
                to_write = self._write_buf
                self._write_buf = b""
                self._writer.write(to_write)

            self._write_event.clear()

    @asyncio.coroutine
    def _response_reader(self):
        # handshake
        greeting = yield from self._reader.read(IPROTO_GREETING_SIZE)
        self._salt = base64.decodestring(greeting[64:])[:20]
        self._auth_event.set()

        buf = b""
        while not self._reader.at_eof():
            tmp_buf = yield from self._reader.read(self.aiobuffer_size)
            if not tmp_buf:
                yield from self._do_close(
                    NetworkError(socket.error(errno.ECONNRESET, "Lost connection to server during query")))

            buf += tmp_buf
            len_buf = len(buf)
            curr = 0

            while len_buf - curr >= 5:
                length_pack = buf[curr:curr + 5]
                length = msgpack.unpackb(length_pack)

                if len_buf - curr < 5 + length:
                    break

                body = buf[curr + 5:curr + 5 + length]
                curr += 5 + length

                response = Response(self, body)  # unpack response

                sync = response.sync
                if sync not in self._waiters:
                    logger.error("aio git happens: {r}", response)
                    continue

                waiter = self._waiters[sync]
                if not waiter.cancelled():
                    if response.return_code != 0:
                        waiter.set_exception(DatabaseError(response.return_code, response.return_message))
                    else:
                        waiter.set_result(response)

                del self._waiters[sync]

            # one cut for buffer
            if curr:
                buf = buf[curr:]

        yield from self._do_close(None)

    @asyncio.coroutine
    def _wait_response(self, sync):
        resp = yield from self._waiters[sync]
        # renew request waiter
        self._waiters[sync] = asyncio.Future(loop=self.loop)
        return resp

    @asyncio.coroutine
    def _send_request(self, request):
        assert isinstance(request, Request)

        if not self.connected:
            yield from self.connect()

        sync = request.sync
        for attempt in range(RETRY_MAX_ATTEMPTS):
            waiter = self._waiters[sync]

            # self._writer.write(bytes(request))
            self._write_buf += bytes(request)
            self._write_event.set()

            # read response
            response = yield from waiter
            if response.completion_status != 1:
                return response

            self._waiters[sync] = asyncio.Future(loop=self.loop)
            warn(response.return_message, RetryWarning)

        # Raise an error if the maximum number of attempts have been made
        raise DatabaseError(response.return_code, response.return_message)

    def generate_sync(self):
        self.req_num += 1
        if self.req_num > 10000000:
            self.req_num = 0

        self._waiters[self.req_num] = asyncio.Future(loop=self.loop)
        return self.req_num

    @asyncio.coroutine
    def close(self):
        yield from self._do_close(None)

    def _do_close(self, exc):
        if not self.connected:
            return

        with (yield from self.lock):
            self.connected = False
            self._writer.transport.close()
            self._reader_task.cancel()
            self._reader_task = None

            self._writer_task.cancel()
            self._writer_task = None
            self._write_event = None
            self._write_buf = None

            self._writer = None
            self._reader = None

            self._auth_event = None

            for waiter in self._waiters.values():
                if exc is None:
                    waiter.cancel()
                else:
                    waiter.set_exception(exc)

            self._waiters = dict()

    def __repr__(self):
        return "aiotarantool.Connection(host=%r, port=%r)" % (self.host, self.port)

    @asyncio.coroutine
    def call(self, func_name, *args):
        assert isinstance(func_name, str)

        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            args = args[0]

        resp = yield from self._send_request(RequestCall(self, func_name, args))
        return resp

    @asyncio.coroutine
    def eval(self, expr, *args):
        assert isinstance(expr, str)

        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            args = args[0]

        resp = yield from self._send_request(RequestEval(self, expr, args))
        return resp

    @asyncio.coroutine
    def replace(self, space_name, values):
        if isinstance(space_name, str):
            sp = yield from self.schema.get_space(space_name)
            space_name = sp.sid

        resp = yield from self._send_request(RequestReplace(self, space_name, values))
        return resp

    @asyncio.coroutine
    def authenticate(self, user, password):
        self.user = user
        self.password = password

        if not self.connected:
            yield from self.connect()

        resp = yield from self._send_request(RequestAuthenticate(self, self._salt, self.user, self.password))
        return resp

    @asyncio.coroutine
    def join(self, server_uuid):
        request = RequestJoin(self, server_uuid)
        resp = yield from self._send_request(request)
        while True:
            yield resp
            if resp.code == REQUEST_TYPE_OK or resp.code >= REQUEST_TYPE_ERROR:
                return

            resp = yield from self._wait_response(request.sync)

        # close connection after JOIN
        yield from self.close()

    @asyncio.coroutine
    def subscribe(self, cluster_uuid, server_uuid, vclock=None):
        vclock = vclock or dict()
        request = RequestSubscribe(self, cluster_uuid, server_uuid, vclock)
        resp = yield from self._send_request(request)
        while True:
            yield resp
            if resp.code >= REQUEST_TYPE_ERROR:
                return

            resp = yield from self._wait_response(request.sync)

        # close connection after SUBSCRIBE
        yield from self.close()

    @asyncio.coroutine
    def insert(self, space_name, values):
        if isinstance(space_name, str):
            sp = yield from self.schema.get_space(space_name)
            space_name = sp.sid

        res = yield from self._send_request(RequestInsert(self, space_name, values))
        return res

    @asyncio.coroutine
    def select(self, space_name, key=None, **kwargs):
        offset = kwargs.get("offset", 0)
        limit = kwargs.get("limit", 0xffffffff)
        index_name = kwargs.get("index", 0)
        iterator_type = kwargs.get("iterator", 0)

        key = check_key(key, select=True)

        if isinstance(space_name, str):
            sp = yield from self.schema.get_space(space_name)
            space_name = sp.sid

        if isinstance(index_name, str):
            idx = yield from self.schema.get_index(space_name, index_name)
            index_name = idx.iid

        res = yield from self._send_request(
            RequestSelect(self, space_name, index_name, key, offset, limit, iterator_type))

        return res

    @asyncio.coroutine
    def delete(self, space_name, key, **kwargs):
        index_name = kwargs.get("index", 0)

        key = check_key(key)
        if isinstance(space_name, str):
            sp = yield from self.schema.get_space(space_name)
            space_name = sp.sid

        if isinstance(index_name, str):
            idx = yield from self.schema.get_index(space_name, index_name)
            index_name = idx.iid

        res = yield from self._send_request(
            RequestDelete(self, space_name, index_name, key))

        return res

    @asyncio.coroutine
    def update(self, space_name, key, op_list, **kwargs):
        index_name = kwargs.get("index", 0)

        key = check_key(key)
        if isinstance(space_name, str):
            sp = yield from self.schema.get_space(space_name)
            space_name = sp.sid

        if isinstance(index_name, str):
            idx = yield from self.schema.get_index(space_name, index_name)
            index_name = idx.iid

        res = yield from self._send_request(
            RequestUpdate(self, space_name, index_name, key, op_list))

        return res

    @asyncio.coroutine
    def ping(self, notime=False):
        request = RequestPing(self)
        t0 = self.loop.time()
        yield from self._send_request(request)
        t1 = self.loop.time()

        if notime:
            return "Success"

        return t1 - t0

