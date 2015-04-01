# -*- coding: utf-8 -*-

import os
import asyncio
import aiotarantool
import string
import multiprocessing


class Bench(object):
    def __init__(self, aiotnt):
        self.tnt = aiotnt
        self.mod_len = len(string.printable)
        self.data = [string.printable[it] * 1536 for it in range(self.mod_len)]

        self.cnt_i = 0
        self.cnt_s = 0
        self.cnt_u = 0
        self.cnt_d = 0

        self.iter_max = 10000

    @asyncio.coroutine
    def insert_job(self):
        for it in range(self.iter_max):
            try:
                yield from self.tnt.insert("tester", (it, self.data[it % self.mod_len]))
                self.cnt_i += 1
            except self.tnt.DatabaseError:
                pass

    @asyncio.coroutine
    def select_job(self):
        for it in range(self.iter_max):
            rs = yield from self.tnt.select("tester", it)
            if len(rs):
                self.cnt_s += 1

    @asyncio.coroutine
    def update_job(self):
        for it in range(self.iter_max):
            try:
                yield from self.tnt.update("tester", it, [("=", 2, it)])
                self.cnt_u += 1
            except self.tnt.DatabaseError:
                pass

    @asyncio.coroutine
    def delete_job(self):
        for it in range(0, self.iter_max, 2):
            rs = yield from self.tnt.delete("tester", it)
            if len(rs):
                self.cnt_d += 1


def target_bench(loop):
    print("run process:", os.getpid())
    tnt = aiotarantool.connect("127.0.0.1", 3301)
    bench = Bench(tnt)

    tasks = []
    tasks += [asyncio.async(bench.insert_job())
              for _ in range(20)]

    tasks += [asyncio.async(bench.select_job())
              for _ in range(20)]

    tasks += [asyncio.async(bench.update_job())
              for _ in range(20)]

    tasks += [asyncio.async(bench.delete_job())
              for _ in range(20)]

    t1 = loop.time()
    loop.run_until_complete(asyncio.wait(tasks))
    t2 = loop.time()

    loop.run_until_complete(tnt.close())

    print("select=%d; insert=%d; update=%d; delete=%d; total=%d" % (
        bench.cnt_s, bench.cnt_i, bench.cnt_u, bench.cnt_d, t2 - t1))

    loop.close()


loop = asyncio.get_event_loop()

workers = [multiprocessing.Process(target=target_bench, args=(loop,))
           for _ in range(22)]

for worker in workers:
    worker.start()

for worker in workers:
    worker.join()
