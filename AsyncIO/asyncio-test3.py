import asyncio
import random
from time import sleep

def task(pid):
    sleep(random.randint(0, 2) * 0.001)
    print(f"Task {pid} done")


async def task_coro(pid):
    await asyncio.sleep(random.randint(0, 2) * 0.001)
    print(f"Task {pid} done")


def synchronous():
    for i in range(1, 10):
        task(i)


async def asynchronous():
    tasks = [asyncio.ensure_future(task_coro(i)) for i in range(1, 10)]
    await asyncio.wait(tasks)


print("Synchronous:")
synchronous()

print("Aynchronous:")
loop = asyncio.get_event_loop()
loop.run_until_complete(asynchronous())
loop.close()