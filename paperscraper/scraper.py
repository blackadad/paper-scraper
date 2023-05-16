from .headers import get_header
from .utils import ThrottledClientSession, check_pdf
from dataclasses import dataclass


@dataclass
class ScraperFunction:
    function: callable
    priority: int
    kwargs: dict
    name: str


class Scraper:
    scrapers = []
    sorted_scrapers = []

    def register_scraper(self, func, attach_session=False, priority=10, name=None):
        kwargs = {}
        if name is None:
            name = func.__name__
        if attach_session:
            sess = ThrottledClientSession(rate_limit=15 / 60, headers=get_header())
            kwargs["session"] = sess
        self.scrapers.append(ScraperFunction(func, priority, kwargs, name))
        # reshape sorted scrapers
        sorted_scrapers = []
        for priority in sorted(set([s.priority for s in self.scrapers])):
            sorted_scrapers.append([s for s in self.scrapers if s.priority == priority])
        self.sorted_scrapers = sorted_scrapers

    async def scrape(self, paper, path, i=0, logger=None) -> bool:
        """Scrape a paper which contains data from Semantic Scholar API.

        Args:
            paper (dict): A paper object from Semantic Scholar API.
            path (str): The path to save the paper.
            i (int): An index to shift call order to load balance.
        """
        # want highest priority first
        for scrapers in self.sorted_scrapers[::-1]:
            for j in range(len(scrapers)):
                j = (j + i) % len(scrapers)
                scraper = scrapers[j]
                try:
                    await scraper.function(paper, path, **scraper.kwargs)
                    if check_pdf(path):
                        return True
                except Exception as e:
                    if logger is not None:
                        logger.info(f"Scraper {scraper.name} failed: {e}")

    async def close(self):
        for scrapers in self.sorted_scrapers:
            for scraper in scrapers:
                if "session" in scraper.kwargs:
                    await scraper.kwargs["session"].close()
