from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

from paperscraper.lib import parse_semantic_scholar_metadata

from .headers import get_header
from .utils import ThrottledClientSession, check_pdf


@dataclass
class ScraperFunction:
    function: Callable[..., Awaitable[bool]]
    priority: int
    kwargs: dict
    name: str
    check_pdf: bool

    def __str__(self) -> str:
        return f"{self.name} - {self.priority}"


class Scraper:
    def __init__(
        self, callback: Callable[[str, dict[str, str]], Awaitable] | None = None
    ):
        self.scrapers = []
        self.sorted_scrapers = []
        self.callback = callback

    def register_scraper(
        self,
        func,
        attach_session: bool = False,
        priority: int = 10,
        name: str | None = None,
        check: bool = True,
        rate_limit: float | None = 15 / 60,
    ) -> None:
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
            path: The path to save the paper.
            i: Optional index (e.g. batch index of the papers) used to shift
                the call order to load balance (e.g. 0 starts at scraper
                function 0, batch 1 starts at scraper function 1, etc.)
            logger: Optional logger to log the scraping process.
        """
        # want highest priority first
        scrape_result = {s.name: "none" for s in self.scrapers}
        for scrapers in self.sorted_scrapers[::-1]:
            for j in range(len(scrapers)):
                scraper = scrapers[(i + j) % len(scrapers)]
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
        return False

    async def batch_scrape(
        self,
        papers: list[dict[str, Any]],
        paper_file_dump_dir: str | os.PathLike,
        batch_index: int = 0,
        logger: logging.Logger | None = None,
    ) -> list[tuple[str, dict[str, Any]] | Literal[False]]:
        """
        Scrape given a list of raw Semantic Scholar information.

        Args:
            papers: List of raw Semantic Scholar paper metadata.
            paper_file_dump_dir: Directory where papers will be downloaded.
            batch_index: Optional batch index of the papers, see scrape's
                docstring for more info.
            logger: Optional logger to log the scraping process.

        Returns:
            List of two-tuples containing the path to the downloaded paper and
                the parsed paper metadata if successful scrape, or False if the
                paper scraping was unsuccessful.
        """

        async def scrape_parse(
            paper: dict[str, Any], i: int
        ) -> tuple[str, dict[str, Any]] | Literal[False]:
            path = os.path.join(paper_file_dump_dir, f'{paper["paperId"]}.pdf')
            success = await self.scrape(paper, path, i=i, logger=logger)
            return (path, parse_semantic_scholar_metadata(paper)) if success else False

        return await asyncio.gather(
            *(scrape_parse(paper=p, i=batch_index + j) for j, p in enumerate(papers))
        )

    async def close(self) -> None:
        for scraper in self.scrapers:
            if "session" in scraper.kwargs:
                await scraper.kwargs["session"].close()
