# -*- coding: utf-8 -*-

import asyncio
import aiotarantool

import logging
logging.basicConfig(level=logging.DEBUG)

import string

mod_len = len(string.printable)
data = [string.printable[it] * 1536 for it in range(mod_len)]

cnt = 0


@asyncio.coroutine
def insert_job(tnt):
    global cnt

    for it in range(2500):
        cnt += 1
        r = yield from tnt.insert("tester", (cnt, data[it % mod_len]))

@asyncio.coroutine
def delete_job(tnt):
    global cnt

    for it in range(2500):
        cnt += 1
        r = yield from tnt.delete("tester", cnt)

loop = asyncio.get_event_loop()

# create tnt instance, not real connect
tnt = aiotarantool.connect("127.0.0.1", 3301)

tasks = [asyncio.async(delete_job(tnt))
         for _ in range(40)]

t1 = loop.time()
loop.run_until_complete(asyncio.wait(tasks))

# close tnt connection
loop.run_until_complete(tnt.close())
t2 = loop.time()

print("total:", t2 - t1)

loop.close()

