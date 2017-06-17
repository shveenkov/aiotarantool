# -*- coding: utf-8 -*-

__version__ = "1.1.5"

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
    RequestUpsert,
    RequestAuthenticate)

from tarantool.schema import SchemaIndex, SchemaSpace
from tarantool.error import SchemaError
import tarantool.const
from tarantool.utils import check_key

from tarantool.error import (
    NetworkError,
    DatabaseError,
    SchemaReloadException)

from tarantool.const import (
    REQUEST_TYPE_OK,
    REQUEST_TYPE_ERROR,
    IPROTO_GREETING_SIZE,
    ENCODING_DEFAULT,
    IPROTO_SYNC)


import logging

logger = logging.getLogger(__package__)


def connect(host, port, user=None, password=None, loop=None, encoding=ENCODING_DEFAULT):
    conn = Connection(host, port, user=user, password=password, loop=loop, encoding=encoding)

    return conn


class Schema(object):
    def __init__(self, con):
        self.schema = {}
        self.con = con

    async def get_space(self, space):
        try:
            return self.schema[space]
        except KeyError:
            pass

        if not self.con.connected:
            await self.con.connect()

        async with self.con.lock:
            if space in self.schema:
                return self.schema[space]

            _index = (tarantool.const.INDEX_SPACE_NAME
                      if isinstance(space, str)
                      else tarantool.const.INDEX_SPACE_PRIMARY)

            array = await self.con.select(tarantool.const.SPACE_SPACE, space, index=_index)
            if len(array) > 1:
                raise SchemaError('Some strange output from server: \n' + array)

            if len(array) == 0 or not len(array[0]):
                temp_name = ('name' if isinstance(space, str) else 'id')
                raise SchemaError(
                    "There's no space with {1} '{0}'".format(space, temp_name))

            array = array[0]
            return SchemaSpace(array, self.schema)

    async def get_index(self, space, index):
        _space = await self.get_space(space)
        try:
            return _space.indexes[index]
        except KeyError:
            pass

        if not self.con.connected:
            await self.con.connect()

        async with self.con.lock:
            if index in _space.indexes:
                return _space.indexes[index]

            _index = (tarantool.const.INDEX_INDEX_NAME
                      if isinstance(index, str)
                      else tarantool.const.INDEX_INDEX_PRIMARY)

            array = await self.con.select(tarantool.const.SPACE_INDEX, [_space.sid, index], index=_index)

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
        self.lock = asyncio.Lock(loop=self.loop)
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
        self._greeting_event = None
        self._salt = None

        self.error = False  # important not raise exception in response reader
        self.schema = Schema(self)  # need schema with lock
        self.schema_version = 1

    async def connect(self):
        if self.connected:
            return

        async with self.lock:
            if self.connected:
                return

            await self._do_connect()

            self.connected = True

    async def _do_connect(self):
        logger.log(logging.DEBUG, "connecting to %r" % self)
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port, loop=self.loop)

        self._reader_task = self.loop.create_task(self._response_reader())
        self._writer_task = self.loop.create_task(self._response_writer())
        self._write_event = asyncio.Event(loop=self.loop)
        self._write_buf = b""

        self._greeting_event = asyncio.Event(loop=self.loop)

        if self.user and self.password:
            await self._greeting_event.wait()
            await self._authenticate(self.user, self.password)

    async def _response_writer(self):
        while True:
            await self._write_event.wait()

            if self._write_buf:
                to_write = self._write_buf
                self._write_buf = b""
                self._writer.write(to_write)

            self._write_event.clear()

    async def _response_reader(self):
        # handshake
        greeting = await self._reader.read(IPROTO_GREETING_SIZE)
        self._salt = base64.decodestring(greeting[64:])[:20]
        self._greeting_event.set()

        buf = b""
        while not self._reader.at_eof():
            tmp_buf = await self._reader.read(self.aiobuffer_size)
            if not tmp_buf:
                await self._do_close(
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
                try:
                    response = Response(self, body)  # unpack response
                except SchemaReloadException as exp:
                    if self.encoding is not None:
                        unpacker = msgpack.Unpacker(use_list=True, encoding=self.encoding)
                    else:
                        unpacker = msgpack.Unpacker(use_list=True)
                    
                    unpacker.feed(body)
                    header = unpacker.unpack()
                    sync = header.get(IPROTO_SYNC, 0)

                    waiter = self._waiters[sync]
                    if not waiter.cancelled():
                        waiter.set_exception(exp)

                    del self._waiters[sync]
                    
                    self.schema.flush()
                    self.schema_version = exp.schema_version
                    continue

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

        await self._do_close(None)

    async def _wait_response(self, sync):
        resp = await self._waiters[sync]
        # renew request waiter
        self._waiters[sync] = asyncio.Future(loop=self.loop)
        return resp

    async def _send_request(self, request):
        assert isinstance(request, Request)

        if not self.connected:
            await self.connect()
        return (await self._send_request_no_check_connected(request))

    async def _send_request_no_check_connected(self, request):
        while True:
            request_bytes = bytes(request)
            sync = request.sync
            waiter = self._waiters[sync]
            
            self._write_buf += request_bytes
            self._write_event.set()
            
            # read response
            try:
                response = await waiter
            except SchemaReloadException:
                continue
            
            return response

    def generate_sync(self):
        self.req_num += 1
        if self.req_num > 10000000:
            self.req_num = 0

        self._waiters[self.req_num] = asyncio.Future(loop=self.loop)
        return self.req_num

    async def close(self):
        await self._do_close(None)

    async def _do_close(self, exc):
        if not self.connected:
            return

        async with self.lock:
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

            self._greeting_event = None

            for waiter in self._waiters.values():
                if exc is None:
                    waiter.cancel()
                else:
                    waiter.set_exception(exc)

            self._waiters = dict()

    def __repr__(self):
        return "aiotarantool.Connection(host=%r, port=%r)" % (self.host, self.port)

    async def call(self, func_name, *args):
        assert isinstance(func_name, str)

        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            args = args[0]

        resp = await self._send_request(RequestCall(self, func_name, args))
        return resp

    async def eval(self, expr, *args):
        assert isinstance(expr, str)

        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            args = args[0]

        resp = await self._send_request(RequestEval(self, expr, args))
        return resp

    async def replace(self, space_name, values):
        if isinstance(space_name, str):
            sp = await self.schema.get_space(space_name)
            space_name = sp.sid

        resp = await self._send_request(RequestReplace(self, space_name, values))
        return resp

    async def authenticate(self, user, password):
        self.user = user
        self.password = password

        if not self.connected:
            await self.connect()  # connects and authorizes
            return
        else:  # need only to authenticate
            await self._authenticate(user, password)

    async def _authenticate(self, user, password):
        assert self._salt, 'Server salt hasn\'t been received.'
        resp = await self._send_request_no_check_connected(RequestAuthenticate(self, self._salt, user, password))
        return resp

    async def join(self, server_uuid):
        request = RequestJoin(self, server_uuid)
        resp = await self._send_request(request)
        while True:
            await resp
            if resp.code == REQUEST_TYPE_OK or resp.code >= REQUEST_TYPE_ERROR:
                return

            resp = await self._wait_response(request.sync)

        # close connection after JOIN
        await self.close()

    async def subscribe(self, cluster_uuid, server_uuid, vclock=None):
        vclock = vclock or dict()
        request = RequestSubscribe(self, cluster_uuid, server_uuid, vclock)
        resp = await self._send_request(request)
        while True:
            await resp
            if resp.code >= REQUEST_TYPE_ERROR:
                return

            resp = await self._wait_response(request.sync)

        # close connection after SUBSCRIBE
        await self.close()

    async def insert(self, space_name, values):
        if isinstance(space_name, str):
            sp = await self.schema.get_space(space_name)
            space_name = sp.sid

        res = await self._send_request(RequestInsert(self, space_name, values))
        return res

    async def select(self, space_name, key=None, **kwargs):
        offset = kwargs.get("offset", 0)
        limit = kwargs.get("limit", 0xffffffff)
        index_name = kwargs.get("index", 0)
        iterator_type = kwargs.get("iterator", 0)

        key = check_key(key, select=True)

        if isinstance(space_name, str):
            sp = await self.schema.get_space(space_name)
            space_name = sp.sid

        if isinstance(index_name, str):
            idx = await self.schema.get_index(space_name, index_name)
            index_name = idx.iid

        res = await self._send_request(
            RequestSelect(self, space_name, index_name, key, offset, limit, iterator_type))

        return res

    async def delete(self, space_name, key, **kwargs):
        index_name = kwargs.get("index", 0)

        key = check_key(key)
        if isinstance(space_name, str):
            sp = await self.schema.get_space(space_name)
            space_name = sp.sid

        if isinstance(index_name, str):
            idx = await self.schema.get_index(space_name, index_name)
            index_name = idx.iid

        res = await self._send_request(
            RequestDelete(self, space_name, index_name, key))

        return res

    async def update(self, space_name, key, op_list, **kwargs):
        index_name = kwargs.get("index", 0)

        key = check_key(key)
        if isinstance(space_name, str):
            sp = await self.schema.get_space(space_name)
            space_name = sp.sid

        if isinstance(index_name, str):
            idx = await self.schema.get_index(space_name, index_name)
            index_name = idx.iid

        res = await self._send_request(
            RequestUpdate(self, space_name, index_name, key, op_list))

        return res

    async def upsert(self, space_name, tuple_value, op_list, **kwargs):
        index_name = kwargs.get("index", 0)

        if isinstance(space_name, str):
            sp = await self.schema.get_space(space_name)
            space_name = sp.sid

        if isinstance(index_name, str):
            idx = await self.schema.get_index(space_name, index_name)
            index_name = idx.iid

        res = await self._send_request(
            RequestUpsert(self, space_name, index_name, tuple_value, op_list))

        return res

    async def ping(self, notime=False):
        request = RequestPing(self)
        t0 = self.loop.time()
        await self._send_request(request)
        t1 = self.loop.time()

        if notime:
            return "Success"

        return t1 - t0
