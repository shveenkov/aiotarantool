# -*- coding: utf-8 -*-

import time
import tarantool

benchmark = {
    "tarantool": {},
    "aiotarantool": {},
}

cnt = 0

tnt = tarantool.connect("127.0.0.1", 3301)

import string
mod_len = len(string.printable)
data = [string.printable[it] * 1536 for it in range(mod_len)]

# sync benchmark
# insert test
print("tarantool insert test")
t1 = time.time()
for it in range(100000):
    r = tnt.insert("tester", (it, data[it % mod_len]))

t2 = time.time()
benchmark["tarantool"]["insert"] = t2 - t1

# select test
print("tarantool select test")
t1 = time.time()
for it in range(100000):
    r = tnt.select("tester", it)

t2 = time.time()
benchmark["tarantool"]["select"] = t2 - t1

# update test
print("tarantool update test")
t1 = time.time()
for it in range(100000):
    r = tnt.update("tester", it, [("=", 2, it)])


t2 = time.time()
benchmark["tarantool"]["update"] = t2 - t1

# delete test
print("tarantool delete test")
t1 = time.time()
for it in range(100000):
    r = tnt.delete("tester", it)

t2 = time.time()
benchmark["tarantool"]["delete"] = t2 - t1

# gevent benchmark
import asyncio
import aiotarantool


@asyncio.coroutine
def insert_job(tnt):
    global cnt

    for i in range(2500):
        cnt += 1
        yield from tnt.insert("tester", (cnt, data[cnt % mod_len]))


@asyncio.coroutine
def select_job(tnt):
    global cnt

    for i in range(2500):
        cnt += 1
        yield from tnt.select("tester", cnt)


@asyncio.coroutine
def update_job(tnt):
    global cnt

    for i in range(2500):
        cnt += 1
        yield from tnt.update("tester", cnt, [("=", 2, cnt)])


@asyncio.coroutine
def delete_job(tnt):
    global cnt

    for i in range(2500):
        cnt += 1
        yield from tnt.delete("tester", cnt)


loop = asyncio.get_event_loop()

tnt = aiotarantool.connect("127.0.0.1", 3301)

# insert test
print("aiotarantool insert test")
t1 = loop.time()
cnt = 0
tasks = [asyncio.async(insert_job(tnt))
         for _ in range(40)]

loop.run_until_complete(asyncio.wait(tasks))
t2 = loop.time()
benchmark["aiotarantool"]["insert"] = t2 - t1

# select test
print("aiotarantool select test")
t1 = loop.time()
cnt = 0
tasks = [asyncio.async(select_job(tnt))
         for _ in range(40)]

loop.run_until_complete(asyncio.wait(tasks))
t2 = loop.time()
benchmark["aiotarantool"]["select"] = t2 - t1

# update test
print("aiotarantool update test")
t1 = loop.time()
cnt = 0
tasks = [asyncio.async(update_job(tnt))
         for _ in range(40)]

loop.run_until_complete(asyncio.wait(tasks))
t2 = loop.time()
benchmark["aiotarantool"]["update"] = t2 - t1

# delete test
print("aiotarantool delete test")
t1 = loop.time()
cnt = 0
tasks = [asyncio.async(delete_job(tnt))
         for _ in range(40)]

loop.run_until_complete(asyncio.wait(tasks))
t2 = loop.time()
benchmark["aiotarantool"]["delete"] = t2 - t1

loop.run_until_complete(tnt.close())
loop.close()

print("\nbenchmark results:")
print("call    tarantool  aiotarantool")
for k in ("insert", "select", "update", "delete"):
    print("{2:6}: {0:0.6f}  {1:0.6f}".format(benchmark["tarantool"][k], benchmark["aiotarantool"][k], k))
