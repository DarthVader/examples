import asyncio

async def foo():
    print('Start foo()')
    await asyncio.sleep(0)
    print('End foo()')

async def bar():
    print('Start bar()')
    await asyncio.sleep(0)
    print('End bar()')


loop = asyncio.get_event_loop()
tasks = [loop.create_task(foo()), loop.create_task(bar())]
wait_tasks = asyncio.wait(tasks)

loop.run_until_complete(wait_tasks)
loop.close()

