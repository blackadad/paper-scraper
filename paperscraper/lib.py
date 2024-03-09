from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import sys
from collections.abc import Awaitable, Callable
from typing import Any

from .exceptions import DOINotFoundError
from .headers import get_header
from .log_formatter import CustomFormatter
from .scraper import Scraper
from .utils import ThrottledClientSession


def clean_upbibtex(bibtex):
    # WTF Semantic Scholar?
    mapping = {
        "None": "article",
        "Article": "article",
        "JournalArticle": "article",
        "Review": "article",
        "Book": "book",
        "BookSection": "inbook",
        "ConferencePaper": "inproceedings",
        "Conference": "inproceedings",
        "Dataset": "misc",
        "Dissertation": "phdthesis",
        "Journal": "article",
        "Patent": "patent",
        "Preprint": "article",
        "Report": "techreport",
        "Thesis": "phdthesis",
        "WebPage": "misc",
        "Plain": "article",
    }

    if "@None" in bibtex:
        return bibtex.replace("@None", "@article")
    # new format check
    match = re.findall(r"@\['(.*)'\]", bibtex)
    if len(match) == 0:
        match = re.findall(r"@(.*)\{", bibtex)
        bib_type = match[0]
        current = f"@{match[0]}"
    else:
        bib_type = match[0]
        current = f"@['{bib_type}']"
    for k, v in mapping.items():
        # can have multiple
        if k in bib_type:
            bibtex = bibtex.replace(current, f"@{v}")
            break
    return bibtex


def format_bibtex(bibtex, key):
    # WOWOW This is hard to use
    from pybtex.database import parse_string
    from pybtex.style.formatting import unsrtalpha

    style = unsrtalpha.Style()
    try:
        bd = parse_string(clean_upbibtex(bibtex), "bibtex")
    except Exception as e:  # noqa: F841
        return "Ref " + key
    try:
        entry = style.format_entry(label="1", entry=bd.entries[key])
        return entry.text.render_as("text")
    except Exception:
        return bd.entries[key].fields["title"]


async def likely_pdf(response):
    try:
        text = await response.text()
        if "Invalid article ID" in text:
            return False
        if "No paper" in text:
            return False
    except UnicodeDecodeError:
        return True
    return True


async def arxiv_to_pdf(arxiv_id, path, session):
    url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"
    # download
    async with session.get(url, allow_redirects=True) as r:
        if r.status != 200 or not await likely_pdf(r):  # noqa: PLR2004
            raise RuntimeError(f"No paper with arxiv id {arxiv_id}")
        with open(path, "wb") as f:  # noqa: ASYNC101
            f.write(await r.read())


async def link_to_pdf(url, path, session):
    # download
    pdf_link = None
    async with session.get(url, allow_redirects=True) as r:
        if r.status != 200:  # noqa: PLR2004
            raise RuntimeError(f"Unable to download {url}, status code {r.status}")
        if "pdf" in r.headers["Content-Type"]:
            with open(path, "wb") as f:  # noqa: ASYNC101
                f.write(await r.read())
            return
        else:  # noqa: RET505
            # try to find a pdf link
            html_text = await r.text()
            # should have pdf somewhere (could not be at end)
            epdf_link = re.search(r'href="(.*\.epdf)"', html_text)
            if epdf_link is None:
                pdf_link = re.search(r'href="(.*pdf.*)"', html_text)
                # try to find epdf link
                if pdf_link is None:
                    raise RuntimeError(f"No PDF link found for {url}")
                pdf_link = pdf_link.group(1)
            else:
                # strip the epdf
                pdf_link = epdf_link.group(1).replace("epdf", "pdf")

            try:
                async with session.get(
                    pdf_link, allow_redirects=True
                ) as r:  # noqa: PLW2901
                    if r.status != 200:  # noqa: PLR2004
                        raise RuntimeError(
                            f"Unable to download {pdf_link}, status code {r.status}"
                        )
                    if "pdf" in r.headers["Content-Type"]:
                        with open(path, "wb") as f:  # noqa: ASYNC101
                            f.write(await r.read())
                        return
                    raise RuntimeError(f"No PDF found from {pdf_link}")
            except TypeError as exc:
                raise RuntimeError(f"Malformed URL {pdf_link} -- {url}") from exc


async def find_pmc_pdf_link(pmc_id, session):
    url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmc_id}"
    async with session.get(url) as r:
        if r.status != 200:  # noqa: PLR2004
            raise RuntimeError(f"No paper with pmc id {pmc_id}. {url} {r.status}")
        html_text = await r.text()
        pdf_link = re.search(r'href="(.*\.pdf)"', html_text)
        if pdf_link is None:
            raise RuntimeError(f"No PDF link found for pmc id {pmc_id}. {url}")
        return f"https://www.ncbi.nlm.nih.gov{pdf_link.group(1)}"


async def pubmed_to_pdf(pubmed_id, path, session):
    url = f"https://pubmed.ncbi.nlm.nih.gov/{pubmed_id}/"

    async with session.get(url) as r:
        if r.status != 200:  # noqa: PLR2004
            raise RuntimeError(
                f"Error fetching PMC ID for PubMed ID {pubmed_id}. {r.status}"
            )
        html_text = await r.text()
        pmc_id_match = re.search(r"PMC\d+", html_text)
        if pmc_id_match is None:
            raise RuntimeError(f"No PMC ID found for PubMed ID {pubmed_id}.")
        pmc_id = pmc_id_match.group(0)
    pmc_id = pmc_id[3:]
    return await pmc_to_pdf(pmc_id, path, session)


async def pmc_to_pdf(pmc_id, path, session):
    pdf_url = await find_pmc_pdf_link(pmc_id, session)
    async with session.get(pdf_url, allow_redirects=True) as r:
        if r.status != 200 or not await likely_pdf(r):  # noqa: PLR2004
            raise RuntimeError(f"No paper with pmc id {pmc_id}. {pdf_url} {r.status}")
        with open(path, "wb") as f:  # noqa: ASYNC101
            f.write(await r.read())


async def arxiv_scraper(paper, path, session):
    if "ArXiv" not in paper["externalIds"]:
        return False
    arxiv_id = paper["externalIds"]["ArXiv"]
    await arxiv_to_pdf(arxiv_id, path, session)
    return True


async def pmc_scraper(paper, path, session):
    if "PubMedCentral" not in paper["externalIds"]:
        return False
    pmc_id = paper["externalIds"]["PubMedCentral"]
    await pmc_to_pdf(pmc_id, path, session)
    return True


async def pubmed_scraper(paper, path, session):
    if "PubMed" not in paper["externalIds"]:
        return False
    pubmed_id = paper["externalIds"]["PubMed"]
    await pubmed_to_pdf(pubmed_id, path, session)
    return True


async def openaccess_scraper(paper, path, session):
    # NOTE: paper may not have the key 'openAccessPdf', or its value may be None
    url = (paper.get("openAccessPdf") or {}).get("url")
    if not url:
        return False
    await link_to_pdf(url, path, session)
    return True


async def local_scraper(paper, path):  # noqa: ARG001
    return True


def default_scraper(
    callback: Callable[[str, dict[str, str]], Awaitable] | None = None
) -> Scraper:
    scraper = Scraper(callback=callback)
    scraper.register_scraper(arxiv_scraper, attach_session=True, rate_limit=30 / 60)
    scraper.register_scraper(pmc_scraper, rate_limit=30 / 60, attach_session=True)
    scraper.register_scraper(pubmed_scraper, rate_limit=30 / 60, attach_session=True)
    scraper.register_scraper(
        openaccess_scraper, attach_session=True, priority=11, rate_limit=45 / 60
    )
    scraper.register_scraper(local_scraper, attach_session=False, priority=12)
    return scraper


def parse_semantic_scholar_metadata(paper: dict[str, Any]) -> dict[str, Any]:
    """Parse raw paper metadata from Semantic Scholar into a more rich format."""
    bibtex = paper["citationStyles"]["bibtex"]
    key = bibtex.split("{")[1].split(",")[0]
    return {
        "citation": format_bibtex(bibtex, key),
        "key": key,
        "bibtex": clean_upbibtex(bibtex),
        "tldr": paper.get("tldr"),
        "year": paper["year"],
        "url": paper["url"],
        "paperId": paper["paperId"],
        "doi": paper["externalIds"].get("DOI", None),
        "citationCount": paper["citationCount"],
        "title": paper["title"],
    }


async def a_search_papers(  # noqa: C901, PLR0912, PLR0915
    query,
    limit=10,
    pdir=os.curdir,
    semantic_scholar_api_key=None,
    _paths: dict[str | os.PathLike, dict[str, Any]] | None = None,
    _limit=100,
    _offset=0,
    logger=None,
    year=None,
    verbose=False,
    scraper=None,
    batch_size=10,
    search_type="default",
) -> dict[str | os.PathLike, dict[str, Any]]:
    if not os.path.exists(pdir):
        os.mkdir(pdir)
    if logger is None:
        logger = logging.getLogger("paper-scraper")
        logger.setLevel(logging.ERROR)
        if verbose:
            logger.setLevel(logging.DEBUG)
            ch = logging.StreamHandler()
            ch.setFormatter(CustomFormatter())
            logger.addHandler(ch)
    params = {
        "fields": ",".join(
            [
                "citationStyles",
                "externalIds",
                "url",
                "openAccessPdf",
                "year",
                "isOpenAccess",
                "influentialCitationCount",
                "citationCount",
                "title",
            ]
        ),
    }
    if search_type == "default":
        endpoint = "https://api.semanticscholar.org/graph/v1/paper/search"
        params["query"] = query.replace("-", " ")
        params["offset"] = _offset
        params["limit"] = _limit
    elif search_type == "paper":
        endpoint = f"https://api.semanticscholar.org/recommendations/v1/papers/forpaper/{query}"
        params["limit"] = _limit
    elif search_type == "doi":
        endpoint = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{query}"
    elif search_type == "future_citations":
        endpoint = f"https://api.semanticscholar.org/graph/v1/paper/{query}/citations"
        params["limit"] = _limit
    elif search_type == "past_references":
        endpoint = f"https://api.semanticscholar.org/graph/v1/paper/{query}/references"
        params["limit"] = _limit
    elif search_type == "google":
        endpoint = "https://api.semanticscholar.org/graph/v1/paper/search"
        params["limit"] = 1
        google_endpoint = "https://serpapi.com/search.json"
        google_params = {
            "q": query,
            "api_key": os.environ["SERPAPI_API_KEY"],
            "engine": "google_scholar",
            "num": 20,
            "start": _offset,
            # TODO - add offset and limit here  # noqa: TD004
        }

    if year is not None and search_type == "default":
        # need to really make sure year is correct
        year = year.strip()
        if "-" in year:
            # make sure start/end are valid
            with contextlib.suppress(ValueError):
                start, end = year.split("-")
                if int(start) <= int(end):
                    params["year"] = year
        if "year" not in params:
            logger.warning(f"Could not parse year {year}")

    if year is not None and search_type == "google":
        # need to really make sure year is correct
        year = year.strip()
        if "-" in year:
            # make sure start/end are valid
            try:
                start, end = year.split("-")
                if int(start) <= int(end):
                    google_params["as_ylo"] = start
                    google_params["as_yhi"] = end
            except ValueError:
                pass
        else:
            with contextlib.suppress(ValueError):
                google_params["as_ylo"] = year
                google_params["as_yhi"] = year
        if "as_ylo" not in google_params:
            logger.warning(f"Could not parse year {year}")

    paths = _paths or {}
    scraper = scraper or default_scraper()
    ssheader = get_header()
    if semantic_scholar_api_key is not None:
        ssheader["x-api-key"] = semantic_scholar_api_key
    else:
        # check if it's in the environment
        with contextlib.suppress(KeyError):
            ssheader["x-api-key"] = os.environ["SEMANTIC_SCHOLAR_API_KEY"]
    async with ThrottledClientSession(  # noqa: SIM117
        rate_limit=(
            90 if "x-api-key" in ssheader or search_type == "google" else 15 / 60
        ),
        headers=ssheader,
    ) as ss_session:
        async with ss_session.get(
            url=google_endpoint if search_type == "google" else endpoint,
            params=google_params if search_type == "google" else params,
        ) as response:
            if response.status != 200:  # noqa: PLR2004
                if response.status == 404 and search_type == "doi":  # noqa: PLR2004
                    raise DOINotFoundError(f"DOI {query} not found")
                raise RuntimeError(
                    f"Error searching papers: {response.status} {response.reason} {await response.text()}"  # noqa: E501
                )
            data = await response.json()

            if search_type == "google":
                if "organic_results" not in data:
                    return paths
                papers = data["organic_results"]
                year_extract = re.compile(r"\b\d{4}\b")
                titles = [p["title"] for p in papers]
                years = [None for p in papers]
                for i, p in enumerate(papers):
                    match = year_extract.findall(p["publication_info"]["summary"])
                    if len(match) > 0:
                        years[i] = match[0]

                # get PDF resources
                google_pdf_links = []
                for i, p in enumerate(papers):
                    google_pdf_links.append(None)
                    if "resources" in p:
                        for res in p["resources"]:
                            if "file_format" in res:  # noqa: SIM102
                                if res["file_format"] == "PDF":
                                    google_pdf_links[i] = res["link"]

                # want this separate, since ss is rate_limit for google
                async with ThrottledClientSession(
                    rate_limit=90 if "x-api-key" in ssheader else 15 / 60,
                    headers=ssheader,
                ) as ss_sub_session:
                    # Now we need to reconcile with S2 API these results
                    async def google2s2(
                        title: str, year: str | None, pdf_link
                    ) -> dict[str, Any] | None:
                        local_p = params.copy()
                        local_p["query"] = title.replace("-", " ")
                        if year is not None:
                            local_p["year"] = year
                        async with ss_sub_session.get(
                            url=endpoint, params=local_p
                        ) as response:
                            if response.status != 200:  # noqa: PLR2004
                                logger.warning(
                                    "Error correlating papers from google to semantic scholar:"
                                    f" status {response.status}, reason {response.reason!r},"
                                    f" text {await response.text()!r}."
                                )
                                return None
                            response = await response.json()  # noqa: PLW2901
                            if (  # noqa: SIM102
                                "data" not in response and year is not None
                            ):
                                if response["total"] == 0:
                                    logger.info(
                                        f"{title} | {year} not found. Now trying without year"
                                    )
                                    del local_p["year"]
                                    async with ss_session.get(
                                        url=endpoint, params=local_p
                                    ) as resp:
                                        if resp.status != 200:  # noqa: PLR2004
                                            logger.warning(
                                                "Error correlating papers from google"
                                                "to semantic scholar (no year)"
                                                f"{response.status} {response.reason} {await response.text()}"  # noqa: E501
                                            )
                                        response = await resp.json()  # noqa: PLW2901
                            if "data" in response:
                                if pdf_link is not None:
                                    # google scholar url takes precedence
                                    response["data"][0]["openAccessPdf"] = {
                                        "url": pdf_link
                                    }
                                return response["data"][0]
                            return None

                    responses = await asyncio.gather(
                        *(
                            google2s2(t, y, p)
                            for t, y, p in zip(titles, years, google_pdf_links)
                        )
                    )
                data = {"data": [r for r in responses if r is not None]}
                data["total"] = len(data["data"])
            field = "data"
            if search_type == "paper":
                field = "recommendedPapers"
            elif search_type == "doi":
                data = {"data": [data]}
            if field not in data:
                return paths
            papers = data[field]
            if search_type == "future_citations":
                papers = [p["citingPaper"] for p in papers]
            if search_type == "past_references":
                papers = [p["citedPaper"] for p in papers]
            # resort based on influentialCitationCount - is this good?
            if search_type == "default":
                papers.sort(key=lambda x: x["influentialCitationCount"], reverse=True)
            if search_type in ["default", "google"]:
                logger.info(
                    f"Found {data['total']} papers, analyzing {_offset} to {_offset + len(papers)}"  # noqa: E501
                )

            async def scrape_parse_paper(
                paper: dict[str, Any], i: int
            ) -> tuple[str, dict[str, Any]] | tuple[None, None]:
                path = os.path.join(pdir, f'{paper["paperId"]}.pdf')
                success = await scraper.scrape(paper, path, i=i, logger=logger)
                return (
                    (path, parse_semantic_scholar_metadata(paper))
                    if success
                    else (None, None)
                )

            # batch them, since we may reach desired limit before all done
            for i in range(0, len(papers), batch_size):
                results = await asyncio.gather(
                    *(
                        scrape_parse_paper(p, i + j)
                        for j, p in enumerate(papers[i : i + batch_size])
                    )
                )
                for path, info in results:
                    if path is not None:
                        paths[path] = info
                # if we have enough, stop
                if len(paths) >= limit:
                    break
    if (
        search_type in ["default", "google"]
        and len(paths) < limit
        and _offset + _limit < data["total"]
    ):
        paths.update(
            await a_search_papers(
                query,
                limit=limit,
                pdir=pdir,
                _paths=paths,
                _limit=_limit,
                _offset=_offset + (20 if search_type == "google" else _limit),
                logger=logger,
                year=year,
                verbose=verbose,
                scraper=scraper,
                batch_size=batch_size,
                search_type=search_type,
            )
        )
    if _offset == 0:
        await scraper.close()
    return paths


def search_papers(
    query,
    limit=10,
    pdir=os.curdir,
    semantic_scholar_api_key=None,
    _paths=None,
    _limit=100,
    _offset=0,
    logger=None,
    year=None,
    verbose=False,
    scraper=None,
    batch_size=10,
    search_type="default",
):
    # special case for jupyter notebooks
    if "get_ipython" in globals() or "google.colab" in sys.modules:
        import nest_asyncio

        nest_asyncio.apply()
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError as e:  # noqa: F841
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(
        a_search_papers(
            query,
            limit=limit,
            pdir=pdir,
            semantic_scholar_api_key=semantic_scholar_api_key,
            _paths=_paths,
            _limit=_limit,
            _offset=_offset,
            logger=logger,
            year=year,
            verbose=verbose,
            scraper=scraper,
            batch_size=batch_size,
            search_type=search_type,
        )
    )
