import asyncio
import contextlib
import os
import random
import re
import time
import urllib.parse
from logging import Logger
from typing import Optional, Union

import aiohttp
import fitz


class ThrottledClientSession(aiohttp.ClientSession):
    """
    Rate-throttled client session class inherited from aiohttp.ClientSession).

    USAGE:
        replace `session = aiohttp.ClientSession()`
        with `session = ThrottledClientSession(rate_limit=15)`
    """

    MIN_SLEEP = 0.001

    def __init__(
        self, rate_limit: Optional[float] = None, *args, **kwargs  # noqa: FA100
    ) -> None:
        # rate_limit - per second
        super().__init__(*args, **kwargs)
        self.rate_limit = rate_limit
        self._fillerTask = None
        self._queue = None
        self._start_time = time.time()
        if rate_limit is not None:
            if rate_limit <= 0:
                raise ValueError("rate_limit must be positive")
            self._queue = asyncio.Queue(max(2, int(rate_limit)))
            self._fillerTask = asyncio.create_task(self._filler(rate_limit))

    def _get_sleep(self) -> Optional[float]:  # noqa: FA100
        if self.rate_limit is not None:
            return max(1 / self.rate_limit, self.MIN_SLEEP)
        return None

    async def close(self) -> None:
        """Close rate-limiter's "bucket filler" task."""
        if self._fillerTask is not None:
            self._fillerTask.cancel()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._fillerTask, timeout=0.5)
        await super().close()

    async def _filler(self, rate_limit: float = 1):
        """Filler task to fill the leaky bucket algo."""
        try:
            if self._queue is None:
                return
            self.rate_limit = rate_limit
            sleep = self._get_sleep()
            updated_at = time.monotonic()
            while True:
                now = time.monotonic()
                # Calculate how many tokens to add to the bucket based on elapsed time.
                tokens_to_add = int((now - updated_at) * rate_limit)
                for _ in range(tokens_to_add):
                    try:
                        # Use 'put_nowait' inside a try-except to handle full queue.
                        self._queue.put_nowait(None)
                    except asyncio.QueueFull:
                        break  # The bucket is full, no more tokens can be added.
                updated_at = now
                await asyncio.sleep(sleep)
        except asyncio.CancelledError:
            pass
        except Exception as err:
            print(str(err))

    async def _allow(self) -> None:
        if self._queue is not None:
            await self._queue.get()
            self._queue.task_done()

    async def _request(self, *args, **kwargs) -> aiohttp.ClientResponse:
        """Throttled _request()."""
        for retries in range(5):
            await self._allow()
            response = await super()._request(*args, **kwargs)
            if response and (response.status in (429, 503, 504)):
                # some service limit reached
                await asyncio.sleep(
                    max(3 * self.rate_limit, (2**retries) * 0.1 + random.random() * 0.1)
                )
                continue
            break
        return response


def check_pdf(path: str, verbose: Union[bool, Logger] = False) -> bool:
    if not os.path.exists(path):
        return False

    try:
        # Open the PDF file using fitz
        with fitz.open(path) as doc:
            # You can add more checks here if needed, e.g., to iterate through pages
            pass  # For now, just opening the file is our basic check

    except Exception as e:  # Catching a general exception as fitz might throw various types of exceptions
        # Handle the verbose logging or printing
        if verbose and isinstance(verbose, bool):
            print(f"PDF at {path} is corrupt or unreadable: {e}")
        elif verbose:
            verbose.exception(f"PDF at {path} is corrupt or unreadable.", exc_info=e)
        return False

    return True



pattern = r"10.\d{4,9}/[-._;():A-Z0-9]+"
compiled_pattern = re.compile(pattern, re.IGNORECASE)


def find_doi(text):
    # https://www.crossref.org/blog/dois-and-matching-regular-expressions/
    match = compiled_pattern.search(text)
    if match:
        proposed = match.group()
    else:
        return None

    # strip off any trailing marksers
    proposed = proposed.replace(".abstract", "")
    proposed = proposed.replace(".full-text", "")
    proposed = proposed.replace(".full", "")
    return proposed.replace(".pdf", "")


def get_hostname(url):
    parsed_url = urllib.parse.urlparse(url)
    return parsed_url.netloc
