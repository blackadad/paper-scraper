from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

from .headers import get_header
from .utils import ThrottledClientSession, aidentity_fn, check_pdf


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
        self.scrapers: list[ScraperFunction] = []
        self.sorted_scrapers: list[list[ScraperFunction]] = []
        self.callback = callback

    def register_scraper(
        self,
        func,
        attach_session: bool = False,
        priority: int = 10,
        name: str | None = None,
        check: bool = True,
        **attached_session_kwargs,
    ) -> None:
        kwargs = {}
        if name is None:
            name = func.__name__.replace("_scraper", "")
        if attach_session:
            kwargs["session"] = ThrottledClientSession(
                **({"headers": get_header()} | attached_session_kwargs)
            )
        self.scrapers.append(ScraperFunction(func, priority, kwargs, name, check))
        # sort scrapers by priority
        self.scrapers.sort(key=lambda x: x.priority, reverse=True)
        # reshape into sorted scrapers
        self._build_sorted_scrapers()

    try:
        SCRAPE_FUNCTION_TIMEOUT: ClassVar[float | None] = float(  # sec
            os.environ.get("PAPERSCRAPER_SCRAPE_FUNCTION_TIMEOUT", "60")
        )
    except ValueError:  # Defeat by setting to "None"
        SCRAPE_FUNCTION_TIMEOUT = None

    def _build_sorted_scrapers(self) -> None:
        self.sorted_scrapers = [
            [s for s in self.scrapers if s.priority == priority]
            for priority in sorted({s.priority for s in self.scrapers})
        ]

    def deregister_scraper(self, name: str) -> None:
        """Remove a scraper by name."""
        for i, scraper in enumerate(self.scrapers):
            if scraper.name == name:
                self.scrapers.pop(i)
                break
        self._build_sorted_scrapers()

    async def scrape(
        self,
        paper: dict[str, Any],
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
                    result = await asyncio.wait_for(
                        scraper.function(paper, path, **scraper.kwargs),
                        timeout=self.SCRAPE_FUNCTION_TIMEOUT,
                    )
                    if result and (
                        not scraper.check_pdf or check_pdf(path, logger or False)
                    ):
                        scrape_result[scraper.name] = "success"
                        if logger is not None:
                            logger.debug(
                                f"\tsucceeded - key: {paper['paperId']} scraper:"
                                f" {scraper.name}"
                            )
                        if self.callback is not None:
                            await self.callback(paper["title"], scrape_result)
                        return True
                except Exception:
                    if logger is not None:
                        logger.exception(
                            f"\tScraper {scraper.name} failed on paper titled"
                            f" {paper.get('title')!r}."
                        )
                scrape_result[scraper.name] = "failed"
            if self.callback is not None:
                await self.callback(paper["title"], scrape_result)
        return False

    async def batch_scrape(
        self,
        papers: Sequence[dict[str, Any]],
        paper_file_dump_dir: str | os.PathLike,
        paper_preprocessor: Callable[[Any], Awaitable[dict[str, Any]]] = aidentity_fn,
        paper_parser: Callable[
            [dict[str, Any]], Awaitable[dict[str, Any]]
        ] = aidentity_fn,
        batch_size: int = 10,
        limit: int | None = None,
        logger: logging.Logger | None = None,
    ) -> dict[str, dict[str, Any]]:
        """
        Scrape given a list of metadata.

        Args:
            papers: List of raw paper metadata.
            paper_file_dump_dir: Directory where papers will be downloaded.
            paper_preprocessor: Optional async function to process the raw paper
                metadata before scraping.
            paper_parser: Optional async function to process the raw paper metadata
                after scraping.
            batch_size: Batch size to use when scraping, within a batch
                scraping is parallelized.
            limit: Optional limit to the number of papers to scrape.
            logger: Optional logger to log the scraping process.

        Returns:
            Dictionary mapping path to downloaded paper to parsed metadata.
        """

        async def scrape_parse(
            paper: dict[str, Any], i: int
        ) -> tuple[str, dict[str, Any]] | Literal[False]:
            try:
                paper = await paper_preprocessor(paper)
            except RuntimeError:  # Failed to hydrate the required paperId
                if logger is not None:
                    logger.exception(f"Failed to preprocess paper {paper}.")
                return False
            path = os.path.join(paper_file_dump_dir, f'{paper["paperId"]}.pdf')
            success = await self.scrape(paper, path, i=i, logger=logger)
            try:
                return (path, await paper_parser(paper)) if success else False
            except RuntimeError:
                # RuntimeError: failed to traverse link inside paper details,
                # or paper is missing field required for parsing like BibTeX links
                if logger is not None:
                    logger.exception(
                        f"Failed to parse paper titled {paper.get('title')!r} with key"
                        f" {paper.get('paperId')!r}."
                    )
                return False

        aggregated: dict[str, dict[str, Any]] = {}
        for i in range(0, len(papers), batch_size):
            aggregated |= {
                r[0]: r[1]
                for r in await asyncio.gather(*(
                    scrape_parse(paper=p, i=i + j)
                    for j, p in enumerate(papers[i : i + batch_size])
                ))
                if r is not False
            }
            if limit is not None and len(aggregated) >= limit:
                break
        return aggregated

    async def close(self) -> None:
        for scraper in self.scrapers:
            if "session" in scraper.kwargs:
                await scraper.kwargs["session"].close()
