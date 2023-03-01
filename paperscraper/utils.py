import asyncio
import datetime
import os
import time
from typing import Optional
import aiohttp


class ThrottledClientSession(aiohttp.ClientSession):
    """
    Rate-throttled client session class inherited from aiohttp.ClientSession)

    USAGE:
        replace `session = aiohttp.ClientSession()`
        with `session = ThrottledClientSession(limit_count=15, limit_seconds=1)`

    see https://stackoverflow.com/a/60357775/107049
    """

    def __init__(self, limit_count: int, limit_seconds: float, min_seconds_between: float = 0, *args, **kwargs) -> None:
        # rate_limit - think it's per second?!
        super().__init__(*args, **kwargs)
        self.limit_count = limit_count
        self.limit_seconds = limit_seconds
        self.min_seconds_between = min_seconds_between
        self._lock = asyncio.Lock()
        self._last_request_time = time.time()
        self._last_period_time = self._get_period_start(time.time())
        self._accumulated_requests = 0

    async def close(self) -> None:
        async with self._lock:
            await super().close()

    def _get_period_start(self, now):
        midnight = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        midnight_ts = midnight.timestamp()
        periods_since_midnight = ((now - midnight_ts) // self.limit_seconds)
        return periods_since_midnight * self.limit_seconds + midnight_ts

    async def _request(self, *args, **kwargs) -> aiohttp.ClientResponse:
        """Throttled _request()"""
        async with self._lock:
            now = time.time()
            next_period = self._last_period_time + self.limit_seconds
            if now < next_period and self._accumulated_requests >= self.limit_count:
                await asyncio.sleep(next_period - now)
                now = time.time()
            if now >= next_period:
                self._last_period_time = self._get_period_start(now)
                self._accumulated_requests = 0
                assert self._last_period_time >= next_period
            if now - self._last_request_time < self.min_seconds_between:
                await asyncio.sleep(self._last_request_time + self.min_seconds_between - now)
            
            result = await super()._request(*args, **kwargs)

            self._last_request_time = time.time()

        return result
