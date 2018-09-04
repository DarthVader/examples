import time
import asyncio


class Timer:
    def __init__(self):
        self.start = time.time()

    def tic(self):
        return "at %1.1f seconds" % (time.time() - self.start)


async def task1():
    timer = Timer()
    print(f"task1 started at: {timer.tic()}")
    await asyncio.sleep(2)
    print(f"task1 ended at: {timer.tic()}")


async def task2():
    timer = Timer()
    print(f"task2 started at: {timer.tic()}")
    await asyncio.sleep(3) # wait 3 seconds!
    print(f"task2 ended at: {timer.tic()}")

async def task3():
    timer = Timer()
    print(f"task3 started at: {timer.tic()}")
    await asyncio.sleep(2)
    print(f"task3 ended at: {timer.tic()}")
    

tasks = [task1(), task2(), task3()]

loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.wait(tasks))
loop.close()


