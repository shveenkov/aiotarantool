# -*- coding: utf-8 -*-

import time
import tarantool

benchmark = {
    "tarantool": {},
    "tornado": {},
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

# tornado benchmark
from tornado.ioloop import IOLoop
import tornado.gen
import tarantool.tornado


@tornado.gen.coroutine
def insert_job(tnt):
    global cnt

    for it in range(2500):
        cnt += 1
        r = yield tnt.insert("tester", (cnt, data[it % mod_len]))


@tornado.gen.coroutine
def select_job(tnt):
    global cnt

    for i in range(2500):
        cnt += 1
        r = yield tnt.select("tester", cnt)


@tornado.gen.coroutine
def update_job(tnt):
    global cnt

    for i in range(2500):
        cnt += 1
        r = yield tnt.update("tester", cnt, [("=", 2, cnt)])


@tornado.gen.coroutine
def delete_job(tnt):
    global cnt

    for i in range(2500):
        cnt += 1
        r = yield tnt.delete("tester", cnt)


loop = IOLoop.current()
tnt = tarantool.tornado.connect("127.0.0.1", 3301)


@tornado.gen.coroutine
def runner1():
    # create tnt instance, not real connect
    yield [insert_job(tnt) for n in range(40)]

@tornado.gen.coroutine
def runner2():
    # create tnt instance, not real connect
    yield [select_job(tnt) for n in range(40)]

@tornado.gen.coroutine
def runner3():
    # create tnt instance, not real connect
    yield [update_job(tnt) for n in range(40)]

@tornado.gen.coroutine
def runner4():
    # create tnt instance, not real connect
    yield [delete_job(tnt) for n in range(40)]

# insert test
print("tornado insert test")
t1 = loop.time()
cnt = 0
loop.run_sync(runner1)
t2 = loop.time()
benchmark["tornado"]["insert"] = t2 - t1

# select test
print("tornado select test")
t1 = loop.time()
cnt = 0
loop.run_sync(runner2)
t2 = loop.time()
benchmark["tornado"]["select"] = t2 - t1

# update test
print("tornado update test")
t1 = loop.time()
cnt = 0
loop.run_sync(runner3)
t2 = loop.time()
benchmark["tornado"]["update"] = t2 - t1

# delete test
print("tornado delete test")
t1 = loop.time()
cnt = 0
loop.run_sync(runner4)
t2 = loop.time()
benchmark["tornado"]["delete"] = t2 - t1

loop.close()

print("\nbenchmark results:")
print("call    tarantool  tarantool.tornado")
for k in ("insert", "select", "update", "delete"):
    print("{2:6}: {0:0.6f}  {1:0.6f}".format(benchmark["tarantool"][k], benchmark["tornado"][k], k))
