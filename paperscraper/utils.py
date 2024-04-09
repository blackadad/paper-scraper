from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random
import re
import time
import urllib.parse
from collections.abc import Collection
from logging import Logger
from typing import cast

import aiohttp
import fitz

logger = logging.getLogger(__name__)


class ThrottledClientSession(aiohttp.ClientSession):
    """
    Rate-throttled client session class inherited from aiohttp.ClientSession).

    USAGE:
        replace `session = aiohttp.ClientSession()`
        with `session = ThrottledClientSession(rate_limit=15)`

    """

    MIN_SLEEP = 0.001

    def __init__(self, rate_limit: float | None = None, *args, **kwargs) -> None:
        """
        Initialize.

        Args:
            rate_limit: Optional number of requests per second to throttle.
            *args: Positional arguments to pass to aiohttp.ClientSession.__init__.
            **kwargs: Keyword arguments to pass to aiohttp.ClientSession.__init__.
        """
        super().__init__(*args, **kwargs)
        self.rate_limit = rate_limit
        self._start_time = time.time()
        if rate_limit is not None:
            if rate_limit <= 0:
                raise ValueError("rate_limit must be positive")
            self._queue: asyncio.Queue | None = asyncio.Queue(
                maxsize=max(2, int(rate_limit))
            )
            self._fillerTask: asyncio.Task | None = asyncio.create_task(
                self._filler(rate_limit)
            )
        else:
            self._queue = None
            self._fillerTask = None

    def _get_sleep(self) -> float | None:
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

    async def _filler(self, rate_limit: float = 1) -> None:
        """Filler task to fill the leaky bucket algo."""
        if self._queue is None:
            return
        try:
            self.rate_limit = rate_limit
            sleep = cast(float, self._get_sleep())
            updated_at = time.perf_counter()
            while True:
                now = time.perf_counter()
                # Calculate how many tokens to add to the bucket based on elapsed time.
                requests_to_add = int((now - updated_at) * rate_limit)
                # Calculate available space in the queue to avoid overfilling it.
                available_space = self._queue.maxsize - self._queue.qsize()
                requests_to_add = min(
                    requests_to_add, available_space
                )  # Only add as many requests as there is space.

                for _ in range(requests_to_add):
                    self._queue.put_nowait(
                        None
                    )  # Insert a request (represented as None) into the queue

                updated_at = now
                await asyncio.sleep(sleep)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Unexpected failure in queue filling.")

    async def _allow(self) -> None:
        if self._queue is not None:
            await self._queue.get()
            self._queue.task_done()

    SERVICE_LIMIT_REACHED_STATUS_CODES: Collection[int] = {429, 503, 504}

    async def _request(self, *args, **kwargs) -> aiohttp.ClientResponse:
        """Throttled _request()."""
        for retries in range(5):
            await self._allow()
            response = await super()._request(*args, **kwargs)
            if response.status in self.SERVICE_LIMIT_REACHED_STATUS_CODES:
                if self.rate_limit is not None:
                    await asyncio.sleep(
                        max(
                            3 * self.rate_limit,
                            (2**retries) * 0.1 + random.random() * 0.1,
                        )
                    )
                    continue
                raise NotImplementedError(
                    "Hit a service limit without a rate limit, please specify a rate limit."
                )
            break
        return response


def check_pdf(path: str | os.PathLike, verbose: bool | Logger = False) -> bool:
    path = str(path)
    if not os.path.exists(path):
        return False

    try:
        # Open the PDF file using fitz
        with fitz.open(path):
            pass  # For now, just opening the file is our basic check

    except fitz.FileDataError as e:
        if verbose and isinstance(verbose, bool):
            print(f"PDF at {path} is corrupt or unreadable: {e}")
        elif verbose:
            verbose.exception(f"PDF at {path} is corrupt or unreadable.", exc_info=e)
        return False

    return True


# SEE: https://www.crossref.org/blog/dois-and-matching-regular-expressions/
# Test cases: https://regex101.com/r/xtI5bS/2
pattern = r"10.\d{4,9}(?:[\/\.][a-z]*\d+[-;():\w]*)+"
compiled_pattern = re.compile(pattern, re.IGNORECASE)


def find_doi(text: str) -> str | None:
    match = compiled_pattern.search(text)
    if not match:
        return None
    return match.group()


def get_hostname(url):
    parsed_url = urllib.parse.urlparse(url)
    return parsed_url.netloc
