Tarantool connection driver for work with asyncio
----------------------------------------------------------
Connector required tarantool version 1.6:

    $ pip install aiotarantool

Try it example:

.. code:: python

    import asyncio
    import aiotarantool

    cnt = 0

    async def insert_job(tnt):
        global cnt

        for it in range(2500):
            cnt += 1
            r = await tnt.insert("tester", (cnt, cnt))

    loop = asyncio.get_event_loop()

    tnt = aiotarantool.connect("127.0.0.1", 3301)
    tasks = [loop.create_task(insert_job(tnt))
             for _ in range(40)]

    loop.run_until_complete(asyncio.wait(tasks))
    loop.run_until_complete(tnt.close())
    loop.close()

Under this scheme the aiotarantool driver makes a smaller number of read/write tarantool socket.

See benchmark results time for insert/select/delete 100K tuples on 1.5KBytes:

=========  =========  ==========
call       tarantool  aiotarantool
=========  =========  ==========
insert     35.938047  12.701088
select     24.389748  12.746204
delete     35.224515  13.905095
=========  =========  ==========

