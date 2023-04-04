import os
import re
import pypdf
from pybtex.bibtex import BibTeXEngine
from .headers import get_header
from .utils import ThrottledClientSession
import asyncio


def clean_upbibtex(bibtex):
    # WTF Semantic Scholar?
    mapping = {
        "None": "article",
        "JournalArticle": "article",
        "Review": "article",
        "Book": "book",
        "BookSection": "inbook",
        "ConferencePaper": "inproceedings",
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
    bib_type = re.findall(r"@\['(.*)'\]", bibtex)[0]
    for k, v in mapping.items():
        # can have multiple
        if k in bib_type:
            bibtex = bibtex.replace(f"@['{bib_type}']", f"@{v}")
            break
    return bibtex


def check_pdf(path, verbose=False):
    if not os.path.exists(path):
        return False
    try:
        pdf = pypdf.PdfReader(path)
    except (pypdf.errors.PyPdfError, ValueError) as e:
        if verbose:
            print(f"PDF at {path} is corrupt: {e}")
        return False
    return True


def format_bibtex(bibtex, key):
    # WOWOW This is hard to use
    from pybtex.database import parse_string
    from pybtex.style.formatting import unsrtalpha

    style = unsrtalpha.Style()
    try:
        bd = parse_string(clean_upbibtex(bibtex), "bibtex")
    except Exception:
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
            raise Exception(f"No paper with arxiv id {arxiv_id}")
        with open(path, "wb") as f:
            f.write(await r.read())


async def link_to_pdf(url, path, session):
    # download
    async with session.get(url, allow_redirects=True) as r:
        if r.status != 200:
            raise Exception(f"Unable to download {url}, status code {r.status}")
        with open(path, "wb") as f:
            f.write(await r.read())


async def pmc_to_pdf(pmc_id, path, session):
    url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmc_id}/pdf/"
    # download
    async with session.get(url, allow_redirects=True) as r:
        if r.status != 200 or not await likely_pdf(r):
            raise Exception(f"No paper with pmc id {pmc_id}. {url} {r.status}")
        with open(path, "wb") as f:
            f.write(await r.read())


async def doi_to_pdf(doi, path, session):
    try:
        base = os.environ.get("DOI2PDF")
    except KeyError:
        raise Exception(
            "Please set the environment variable DOI2PDF to a website that can convert a DOI to a PDF."
        )
    if base[-1] == "/":
        base = base[:-1]
    url = f"{base}/{doi}"
    # get to iframe thing
    async with session.get(url, allow_redirects=True) as iframe_r:
        if iframe_r.status != 200:
            raise Exception(f"No paper with doi {doi}")
        # get pdf url by regex
        # looking for button onclick
        try:
            pdf_url = re.search(
                r"location\.href='(.*?download=true)'", await iframe_r.text()
            ).group(1)
        except AttributeError:
            raise Exception(f"No paper with doi {doi}")
    # can be relative or absolute
    if pdf_url.startswith("//"):
        pdf_url = f"https:{pdf_url}"
    else:
        pdf_url = f"{base}{pdf_url}"
    # download
    async with session.get(pdf_url, allow_redirects=True) as r:
        with open(path, "wb") as f:
            f.write(await r.read())


async def a_search_papers(
    query,
    limit=10,
    pdir=os.curdir,
    verbose=False,
    semantic_scholar_api_key=None,
    _paths=None,
    _limit=100,
    _offset=0,
    logger=None,
):
    if not os.path.exists(pdir):
        os.mkdir(pdir)
    if logger is None:
        logger = print
    endpoint = "https://api.semanticscholar.org/graph/v1/paper/search"
    params = {
        "query": query,
        "fields": ",".join(
            [
                "citationStyles",
                "externalIds",
                "url",
                "openAccessPdf",
                "year",
                "isOpenAccess",
                "influentialCitationCount",
                "tldr",
            ]
        ),
        "limit": _limit,
        "offset": _offset,
    }
    if _paths is None:
        paths = {}
    else:
        paths = _paths
    ssheader = get_header()
    if semantic_scholar_api_key is not None:
        ssheader["x-api-key"] = semantic_scholar_api_key
    else:
        # check if its in the environment
        try:
            ssheader["x-api-key"] = os.environ["SEMANTIC_SCHOLAR_API_KEY"]
        except KeyError:
            pass
    have_key = "x-api-key" in ssheader
    async with ThrottledClientSession(
        rate_limit=15 / 60, headers=ssheader
    ) as ss_session, ThrottledClientSession(
        rate_limit=15 / 60, headers=get_header()
    ) as arxiv_session, ThrottledClientSession(
        rate_limit=15 / 60, headers=get_header()
    ) as pmc_session, ThrottledClientSession(
        rate_limit=15 / 60, headers=get_header()
    ) as doi2pdf_session, ThrottledClientSession(
        rate_limit=15 / 60, headers=get_header()
    ) as publisher_session:
        async with ss_session.get(url=endpoint, params=params) as response:
            if response.status != 200:
                raise Exception(
                    f"Error searching papers: {response.status} {response.reason} {await response.text()}"
                )
            data = await response.json()
            papers = data["data"]
            # resort based on influentialCitationCount - is this good?
            papers.sort(key=lambda x: x["influentialCitationCount"], reverse=True)
            if verbose:
                logger(
                    f"Found {data['total']} papers, analyzing {_offset} to {_offset + len(papers)}"
                )

            async def process_paper(paper, i):
                if len(paths) >= limit:
                    return None, None
                path = os.path.join(pdir, f'{paper["paperId"]}.pdf')
                success = check_pdf(path, verbose=verbose)
                if success and verbose:
                    logger("\tfound downloaded version")
                # space them out like so we can balance the load
                sources = [
                    ("ArXiv", arxiv_to_pdf, arxiv_session),
                    ("PubMed", pmc_to_pdf, pmc_session),
                    ("openAccessPdf", link_to_pdf, publisher_session),
                    ("DOI", doi_to_pdf, doi2pdf_session),
                ]

                source = sources[i % len(sources)]

                if source[0] in paper["externalIds"] and not success:
                    try:
                        if source[0] == "openAccessPdf":
                            await source[1](paper[source[0]]["url"], path, source[2])
                        else:
                            await source[1](
                                paper["externalIds"][source[0]], path, source[2]
                            )
                        success = check_pdf(path, verbose=verbose)
                        if verbose and success:
                            logger(f"\t{source[0].lower()} succeeded")
                    except Exception as e:
                        if verbose:
                            logger(f"\t{source[0].lower()} failed")

                if verbose and not success:
                    logger("\tfailed")
                else:
                    bibtex = paper["citationStyles"]["bibtex"]
                    key = bibtex.split("{")[1].split(",")[0]
                    if verbose:
                        logger("\tsucceeded - key: " + key)
                    return path, dict(
                        citation=format_bibtex(bibtex, key),
                        key=key,
                        bibtex=bibtex,
                        tldr=paper["tldr"],
                        year=paper["year"],
                        url=paper["url"],
                    )
                return None, None

            # batch them, since since we may reach desired limit before all done
            batch_size = 10
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
                # if we have enough, stop
                if len(paths) >= limit:
                    break

    if len(paths) < limit and _offset + _limit < data["total"]:
        paths.update(
            await a_search_papers(
                query,
                limit=limit,
                pdir=pdir,
                verbose=verbose,
                _paths=paths,
                _limit=_limit,
                _offset=_offset + _limit,
                logger=logger,
            )
        )
    return paths


def search_papers(
    query,
    limit=10,
    pdir=os.curdir,
    verbose=False,
    semantic_scholar_api_key=None,
    _paths=None,
    _limit=100,
    _offset=0,
    logger=None,
):
    # special case for jupyter notebooks
    if "get_ipython" in globals():
        import nest_asyncio

        nest_asyncio.apply()
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(
        a_search_papers(
            query,
            limit=limit,
            pdir=pdir,
            verbose=verbose,
            semantic_scholar_api_key=semantic_scholar_api_key,
            _paths=_paths,
            _limit=_limit,
            _offset=_offset,
            logger=logger,
        )
    )
