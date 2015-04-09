# -*- coding: utf-8 -*-

from tornado.ioloop import IOLoop
import tornado.gen
import tarantool.tornado

import logging
logging.basicConfig(level=logging.DEBUG)

import string

mod_len = len(string.printable)
data = [string.printable[it] * 1536 for it in range(mod_len)]

cnt = 0

@tornado.gen.coroutine
def insert_job(tnt):
    global cnt

    for it in range(2500):
        cnt += 1
        r = yield tnt.insert("tester", (cnt, data[it % mod_len]))


@tornado.gen.coroutine
def delete_job(tnt):
    global cnt

    for it in range(2500):
        cnt += 1
        r = yield tnt.delete("tester", cnt)


@tornado.gen.coroutine
def runner():
    # create tnt instance, not real connect
    tnt = tarantool.tornado.connect("127.0.0.1", 3301)

    tasks = [insert_job(tnt) for n in range(40)]

    yield tasks

loop = IOLoop.current()

t0 = loop.time()
loop.run_sync(runner)
t1 = loop.time()

print("total:", t1 - t0)
