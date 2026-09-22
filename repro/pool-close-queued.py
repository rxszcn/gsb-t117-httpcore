"""池关闭时还在排队的那条请求会不会醒，六行读数。

跑法：.venv/bin/python repro/pool-close-queued.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import os
import sys
import time

import anyio
import httpcore


BUF = [
    b"HTTP/1.1 200 OK\r\n",
    b"Content-Type: plain/text\r\n",
    b"Content-Length: 13\r\n",
    b"\r\n",
    b"Hello, world!",
]


async def main() -> None:
    pool = httpcore.AsyncConnectionPool(
        network_backend=httpcore.AsyncMockBackend(BUF * 5), max_connections=1
    )

    # Occupy the only connection with a response we do not finish reading.
    # Keep the context-manager object alive: dropping it lets the event loop
    # finalise the async generator and close the response for us.
    held_cm = pool.stream("GET", "https://a.com/")
    held = await held_cm.__aenter__()
    print("held:", repr(pool))

    state = {"returned": False, "exc": None}
    finished = anyio.Event()

    async def queued_request() -> None:
        try:
            # No pool timeout: this is the normal 'await pool.request(...)' shape.
            await pool.request("GET", "https://b.com/")
            state["returned"] = True
        except BaseException as exc:  # noqa: BLE001
            state["exc"] = repr(exc)
        finished.set()

    async with anyio.create_task_group() as tg:
        tg.start_soon(queued_request)
        await anyio.sleep(0.05)
        print("while queued:", repr(pool))

        t0 = time.monotonic()
        await pool.aclose()
        print("pool.aclose() returned after %.3fs" % (time.monotonic() - t0))

        with anyio.move_on_after(1.0) as scope:
            await finished.wait()
        print("queued request finished:", state["returned"])
        print("queued request exception:", state["exc"])
        print("queued request still blocked after pool closed:", scope.cancelled_caught)
        print("after close:", repr(pool))
        tg.cancel_scope.cancel()

    await held.aclose()


anyio.run(main)
