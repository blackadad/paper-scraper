from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from .headers import get_header
from .utils import ThrottledClientSession, check_pdf


@dataclass
class ScraperFunction:
    function: callable
    priority: int
    kwargs: dict
    name: str
    check_pdf: bool

    def __str__(self):
        return f"{self.name} - {self.priority}"


class Scraper:
    def __init__(self, callback=None):
        self.scrapers = []
        self.sorted_scrapers = []
        self.callback = callback

    def register_scraper(
        self,
        func,
        attach_session=False,
        priority=10,
        name=None,
        check=True,
        rate_limit=15 / 60,
    ):
        kwargs = {}
        if name is None:
            name = func.__name__.replace("_scraper", "")
        if attach_session:
            sess = ThrottledClientSession(rate_limit=rate_limit, headers=get_header())
            kwargs["session"] = sess
        self.scrapers.append(ScraperFunction(func, priority, kwargs, name, check))
        # sort scrapers by priority
        self.scrapers.sort(key=lambda x: x.priority, reverse=True)
        # reshape sorted scrapers
        sorted_scrapers = []
        for priority in sorted({s.priority for s in self.scrapers}):
            sorted_scrapers.append(  # noqa: PERF401
                [s for s in self.scrapers if s.priority == priority]
            )
        self.sorted_scrapers = sorted_scrapers

    async def scrape(
        self,
        paper,
        path: str | os.PathLike,
        i: int = 0,
        logger: logging.Logger | None = None,
    ) -> bool:
        """Scrape a paper which contains data from Semantic Scholar API.

        Args:
            paper (dict): A paper object from Semantic Scholar API.
            path (str): The path to save the paper.
            i (int): An index to shift call order to load balance.
            logger (logging.Logger | None): An optional logger to log the scraping process.
        """
        # want highest priority first
        scrape_result = {s.name: "none" for s in self.scrapers}
        for scrapers in self.sorted_scrapers[::-1]:
            for j in range(len(scrapers)):
                j = (j + i) % len(scrapers)  # noqa: PLW2901
                scraper = scrapers[j]
                try:
                    result = await scraper.function(paper, path, **scraper.kwargs)
                    if result and (not scraper.check_pdf or check_pdf(path)):
                        scrape_result[scraper.name] = "success"
                        if logger is not None:
                            logger.debug(
                                f"\tsucceeded - key: {paper['paperId']} scraper: {scraper.name}"
                            )
                        if self.callback is not None:
                            await self.callback(paper["title"], scrape_result)
                        return True
                except Exception as e:
                    if logger is not None:
                        logger.info(f"\tScraper {scraper.name} failed: {e}")
                scrape_result[scraper.name] = "failed"
            if self.callback is not None:
                await self.callback(paper["title"], scrape_result)
        return None

    async def close(self):
        for scraper in self.scrapers:
            if "session" in scraper.kwargs:
                await scraper.kwargs["session"].close()
