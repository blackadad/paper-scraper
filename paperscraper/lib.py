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
            ]
        ),
        "limit": _limit,
        "offset": _offset,
    }
    if _paths is None:
        paths = {}
    else:
        paths = _paths

    async with ThrottledClientSession(
        limit_count=100, limit_seconds=5*60, headers=get_header()
    ) as ss_session, ThrottledClientSession(
        limit_count=15, limit_seconds=1, headers=get_header()
    ) as arxiv_session, ThrottledClientSession(
        limit_count=15, limit_seconds=1, headers=get_header()
    ) as pmc_session, ThrottledClientSession(
        limit_count=15, limit_seconds=1, headers=get_header()
    ) as doi2pdf_session, ThrottledClientSession(
        limit_count=15, limit_seconds=1, headers=get_header()
    ) as publisher_session:
        async with ss_session.get(url=endpoint, params=params) as response:
            if response.status != 200:
                raise Exception(f"Error searching papers: {response.status}")
            data = await response.json()
            papers = data["data"]
            # resort based on influentialCitationCount - is this good?
            papers.sort(key=lambda x: x["influentialCitationCount"], reverse=True)
            if verbose:
                logger(
                    f"Found {data['total']} papers, analyzing {_offset} to {_offset + len(papers)}"
                )
            for i, paper in enumerate(papers):
                if len(paths) >= limit:
                    break
                path = os.path.join(pdir, f'{paper["paperId"]}.pdf')
                success = check_pdf(path, verbose=verbose)
                if success and verbose:
                    logger("\tfound downloaded version")
                if "ArXiv" in paper["externalIds"] and not success:
                    try:
                        await arxiv_to_pdf(
                            paper["externalIds"]["ArXiv"], path, arxiv_session
                        )
                        success = check_pdf(path, verbose=verbose)
                        if verbose and success:
                            logger("\tarxiv succeeded")
                    except Exception as e:
                        if verbose:
                            logger("\tarxiv failed")
                if "PubMed" in paper["externalIds"] and not success:
                    try:
                        await pmc_to_pdf(
                            paper["externalIds"]["PubMed"], path, pmc_session
                        )
                        success = check_pdf(path, verbose=verbose)
                        if verbose and success:
                            logger("\tpmc succeeded")
                    except Exception as e:
                        if verbose:
                            logger("\tpmc failed")
                if "openAccessPdf" in paper and not success:
                    try:
                        await link_to_pdf(
                            paper["openAccessPdf"]["url"], path, publisher_session
                        )
                        success = check_pdf(path, verbose=verbose)
                        if verbose and success:
                            logger("\topen access succeeded")
                    except Exception as e:
                        if verbose:
                            logger("\topen access failed")
                if "DOI" in paper["externalIds"] and not success:
                    try:
                        await doi_to_pdf(
                            paper["externalIds"]["DOI"], path, doi2pdf_session
                        )
                        success = check_pdf(path, verbose=verbose)
                        if verbose and success:
                            logger("\tother succeeded")
                    except Exception as e:
                        if verbose:
                            logger("\tother failed")
                if verbose and not success:
                    logger("\tfailed")
                else:
                    bibtex = paper["citationStyles"]["bibtex"]
                    key = bibtex.split("{")[1].split(",")[0]
                    paths[path] = dict(
                        citation=format_bibtex(bibtex, key), key=key, bibtex=bibtex
                    )
                    if verbose:
                        logger("\tsucceeded - key: " + key)
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
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(
        a_search_papers(
            query,
            limit=limit,
            pdir=pdir,
            verbose=verbose,
            _paths=_paths,
            _limit=_limit,
            _offset=_offset,
            logger=logger,
        )
    )
