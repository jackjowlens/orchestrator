import asyncio


async def periodic(interval, submit):
    """Fixed-delay clock on the engine event loop. Skip a tick at capacity."""
    while True:
        await asyncio.sleep(interval)
        try:
            await submit()
        except BufferError:
            continue
