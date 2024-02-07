import os
import re
import pybtex
from pybtex.bibtex import BibTeXEngine
from .headers import get_header
from .utils import ThrottledClientSession
from .scraper import Scraper
import asyncio
import re
import sys
import logging
from .log_formatter import CustomFormatter
from .exceptions import DOINotFoundError


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
    except Exception as e:
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
        if r.status != 200 or not await likely_pdf(r):
            raise RuntimeError(f"No paper with arxiv id {arxiv_id}")
        with open(path, "wb") as f:
            f.write(await r.read())


async def link_to_pdf(url, path, session):
    # download
    pdf_link = None
    async with session.get(url, allow_redirects=True) as r:
        if r.status != 200:
            raise RuntimeError(f"Unable to download {url}, status code {r.status}")
        if "pdf" in r.headers["Content-Type"]:
            with open(path, "wb") as f:
                f.write(await r.read())
            return
        else:
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
                async with session.get(pdf_link, allow_redirects=True) as r:
                    if r.status != 200:
                        raise RuntimeError(
                            f"Unable to download {pdf_link}, status code {r.status}"
                        )
                    if "pdf" in r.headers["Content-Type"]:
                        with open(path, "wb") as f:
                            f.write(await r.read())
                        return
                    else:
                        raise RuntimeError(f"No PDF found from {pdf_link}")
            except TypeError:
                raise RuntimeError(f"Malformed URL {pdf_link} -- {url}")


async def find_pmc_pdf_link(pmc_id, session):
    url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmc_id}"
    async with session.get(url) as r:
        if r.status != 200:
            raise RuntimeError(f"No paper with pmc id {pmc_id}. {url} {r.status}")
        html_text = await r.text()
        pdf_link = re.search(r'href="(.*\.pdf)"', html_text)
        if pdf_link is None:
            raise RuntimeError(f"No PDF link found for pmc id {pmc_id}. {url}")
        return f"https://www.ncbi.nlm.nih.gov{pdf_link.group(1)}"


async def pubmed_to_pdf(pubmed_id, path, session):
    url = f"https://pubmed.ncbi.nlm.nih.gov/{pubmed_id}/"

    async with session.get(url) as r:
        if r.status != 200:
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
        if r.status != 200 or not await likely_pdf(r):
            raise RuntimeError(f"No paper with pmc id {pmc_id}. {pdf_url} {r.status}")
        with open(path, "wb") as f:
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
    url = paper.get("openAccessPdf", {}).get("url")
    if not url:
        return False
    else:
        await link_to_pdf(url, path, session)
        return True

async def local_scraper(paper, path):
    return True


def default_scraper():
    scraper = Scraper()
    scraper.register_scraper(arxiv_scraper, attach_session=True, rate_limit=30 / 60)
    scraper.register_scraper(pmc_scraper, rate_limit=30 / 60, attach_session=True)
    scraper.register_scraper(pubmed_scraper, rate_limit=30 / 60, attach_session=True)
    scraper.register_scraper(
        openaccess_scraper, attach_session=True, priority=11, rate_limit=45 / 60
    )
    scraper.register_scraper(local_scraper, attach_session=False, priority=12)
    return scraper


async def a_search_papers(
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
        endpoint = "https://api.semanticscholar.org/recommendations/v1/papers/forpaper/{paper_id}".format(
            paper_id=query
        )
        params["limit"] = _limit
    elif search_type == "doi":
        endpoint = "https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}".format(
            doi=query
        )
    elif search_type == "future_citations":
        endpoint = "https://api.semanticscholar.org/graph/v1/paper/{paper_id}/citations".format(
            paper_id=query
        )
        params["limit"] = _limit
    elif search_type == "past_references":
        endpoint = "https://api.semanticscholar.org/graph/v1/paper/{paper_id}/references".format(
            paper_id=query
        )
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
            # TODO - add offset and limit here
        }

    if year is not None and search_type == "default":
        # need to really make sure year is correct
        year = year.strip()
        if "-" in year:
            # make sure start/end are valid
            try:
                start, end = year.split("-")
                if int(start) <= int(end):
                    params["year"] = year
            except ValueError:
                pass
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
            try:
                google_params["as_ylo"] = year
                google_params["as_yhi"] = year
            except ValueError:
                pass
        if "as_ylo" not in google_params:
            logger.warning(f"Could not parse year {year}")

    if _paths is None:
        paths = {}
    else:
        paths = _paths
    if scraper is None:
        scraper = default_scraper()
    ssheader = get_header()
    if semantic_scholar_api_key is not None:
        ssheader["x-api-key"] = semantic_scholar_api_key
    else:
        # check if its in the environment
        try:
            ssheader["x-api-key"] = os.environ["SEMANTIC_SCHOLAR_API_KEY"]
        except KeyError:
            pass
    async with ThrottledClientSession(
        rate_limit=90
        if "x-api-key" in ssheader or search_type == "google"
        else 15 / 60,
        headers=ssheader,
    ) as ss_session:
        async with ss_session.get(
            url=google_endpoint if search_type == "google" else endpoint,
            params=google_params if search_type == "google" else params,
        ) as response:
            if response.status != 200:
                if response.status == 404 and search_type == "doi":
                    raise DOINotFoundError(f"DOI {query} not found")
                raise RuntimeError(
                    f"Error searching papers: {response.status} {response.reason} {await response.text()}"
                )
            data = await response.json()

            if search_type == "google":
                if not "organic_results" in data:
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
                            if "file_format" in res:
                                if res["file_format"] == "PDF":
                                    google_pdf_links[i] = res["link"]

                data = {"data": []}
                # now we mock a s2 request
                # by querying Semantic Scholar with the Google results
                local_p = params.copy()
                for title, year, pdf_link in zip(titles, years, google_pdf_links):
                    local_p["query"] = title.replace("-", " ")
                    if year is not None:
                        local_p["year"] = year
                    async with ss_session.get(url=endpoint, params=local_p) as response:
                        if response.status != 200:
                            logger.warning(
                                f"Error correlating papers from google to semantic scholar"
                                f"{response.status} {response.reason} {await response.text()}"
                            )
                            continue
                        response = await response.json()
                        if "data" not in response and year is not None:
                            if response["total"] == 0:
                                logger.info(
                                    f"{title} | {year} not found. Now trying without year"
                                )
                                del local_p["year"]
                                async with ss_session.get(
                                    url=endpoint, params=local_p
                                ) as resp:
                                    if resp.status != 200:
                                        logger.warning(
                                            "Error correlating papers from google"
                                            "to semantic scholar (no year)"
                                            f"{response.status} {response.reason} {await response.text()}"
                                        )
                                    response = await resp.json()
                        if "data" in response:
                            if pdf_link is not None:
                                # google scholar url takes precedence
                                response["data"][0]["openAccessPdf"] = {"url": pdf_link}
                            data["data"].append(response["data"][0])
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
                    f"Found {data['total']} papers, analyzing {_offset} to {_offset + len(papers)}"
                )

            async def process_paper(paper, i):
                path = os.path.join(pdir, f'{paper["paperId"]}.pdf')
                success = await scraper.scrape(paper, path, i=i, logger=logger)
                if success:
                    bibtex = paper["citationStyles"]["bibtex"]
                    key = bibtex.split("{")[1].split(",")[0]
                    return path, dict(
                        citation=format_bibtex(bibtex, key),
                        key=key,
                        bibtex=clean_upbibtex(bibtex),
                        tldr=paper["tldr"] if "tldr" in paper else None,
                        year=paper["year"],
                        url=paper["url"],
                        paperId=paper["paperId"],
                        doi=paper["externalIds"]["DOI"]
                        if "DOI" in paper["externalIds"]
                        else None,
                        citationCount=paper["citationCount"],
                        title=paper["title"],
                    )
                return None, None

            # batch them, since since we may reach desired limit before all done
            for i in range(0, len(papers), batch_size):
                batch = papers[i : i + batch_size]
                results = await asyncio.gather(
                    *[process_paper(p, i + j) for j, p in enumerate(batch)]
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
    except RuntimeError as e:
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
