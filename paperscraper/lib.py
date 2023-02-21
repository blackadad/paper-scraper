import requests
import os
import re
import pypdf
from requests_ratelimiter import LimiterSession
from pybtex.bibtex import BibTeXEngine
from .headers import get_header

arxiv_session = LimiterSession(per_minute=15)
pmc_session = LimiterSession(per_minute=15)
doi2pdf_session = LimiterSession(per_minute=15)
publisher_session = LimiterSession(per_minute=15)


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


def arxiv_to_pdf(arxiv_id, path):
    url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"
    # download
    r = pmc_session.get(url, allow_redirects=True, headers=get_header())
    if r.status_code != 200 or f"No paper 'arXiv:{arxiv_id}.pdf'" in r.text:
        raise Exception(f"No paper with arxiv id {arxiv_id}")
    with open(path, "wb") as f:
        f.write(r.content)


def link_to_pdf(url, path):
    # download
    r = publisher_session.get(url, allow_redirects=True, headers=get_header())
    if r.status_code != 200:
        raise Exception(f"Unable to download {url}, status code {r.status_code}")
    with open(path, "wb") as f:
        f.write(r.content)


def pmc_to_pdf(pmc_id, path):
    url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmc_id}/pdf/"
    # download
    r = pmc_session.get(url, allow_redirects=True, headers=get_header())
    if r.status_code != 200 or "Invalid article ID" in r.text:
        raise Exception(f"No paper with pmc id {pmc_id}. {url} {r.status_code}")
    with open(path, "wb") as f:
        f.write(r.content)


def doi_to_pdf(doi, path):
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
    iframe_r = doi2pdf_session.get(url, allow_redirects=True)
    if iframe_r.status_code != 200:
        raise Exception(f"No paper with doi {doi}")
    # get pdf url by regex
    # looking for button onclick
    try:
        pdf_url = re.search(
            r"location\.href='(.*?download=true)'", iframe_r.text
        ).group(1)
    except AttributeError:
        raise Exception(f"No paper with doi {doi}")
    # can be relative or absolute
    if pdf_url.startswith("//"):
        pdf_url = f"https:{pdf_url}"
    else:
        pdf_url = f"{base}{pdf_url}"
    print(pdf_url)
    # download
    r = doi2pdf_session.get(pdf_url, allow_redirects=True)
    with open(path, "wb") as f:
        f.write(r.content)


def search_papers(
    query, limit=10, pdir=os.curdir, verbose=False, _paths=None, _limit=100, _offset=0
):
    if not os.path.exists(pdir):
        os.mkdir(pdir)
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
    response = requests.get(endpoint, params=params)
    if _paths is None:
        paths = {}
    else:
        paths = _paths
    if response.status_code == 200:
        data = response.json()
        # resort based on influentialCitationCount
        papers = data["data"]
        papers.sort(key=lambda x: x["influentialCitationCount"], reverse=True)
        for paper in papers:
            if len(paths) >= limit:
                break
            path = os.path.join(pdir, f'{paper["paperId"]}.pdf')
            success = check_pdf(path, verbose=verbose)
            if "ArXiv" in paper["externalIds"] and not success:
                try:
                    arxiv_to_pdf(paper["externalIds"]["ArXiv"], path)
                    success = check_pdf(path, verbose=verbose)
                    if verbose and success:
                        print("Downloaded arxiv")
                except Exception as e:
                    if verbose:
                        print("Failed to download arxiv", e)
            if "PubMed" in paper["externalIds"] and not success:
                try:
                    pmc_to_pdf(paper["externalIds"]["PubMed"], path)
                    success = check_pdf(path, verbose=verbose)
                    if verbose and success:
                        print("Downloaded pmc")
                except Exception as e:
                    if verbose:
                        print("Failed to download pmc", e)
            if "openAccessPdf" in paper and not success:
                try:
                    link_to_pdf(paper["openAccessPdf"]["url"], path)
                    success = check_pdf(path, verbose=verbose)
                    if verbose and success:
                        print("Downloaded openAccessPdf")
                except Exception as e:
                    if verbose:
                        print("Failed to download openAccessPdf", e)
            if "DOI" in paper["externalIds"] and not success:
                try:
                    doi_to_pdf(paper["externalIds"]["DOI"], path)
                    success = check_pdf(path, verbose=verbose)
                    if verbose and success:
                        print("Downloaded doi")
                except Exception as e:
                    if verbose:
                        print("Failed to download other", e)
            if not success:
                print(f'Could not download {paper["paperId"]}')
                print("External IDs:")
                print(paper["externalIds"])
            else:
                bibtex = paper["citationStyles"]["bibtex"]
                key = bibtex.split("{")[1].split(",")[0]
                paths[path] = dict(
                    citation=format_bibtex(bibtex, key), key=key, bibtex=bibtex
                )
                if verbose:
                    print("Succeeded - key:", key)
    if len(paths) < limit and _offset + _limit < data["total"]:
        paths.update(
            search_papers(
                query,
                limit=limit,
                pdir=pdir,
                verbose=verbose,
                _paths=paths,
                _limit=_limit,
                _offset=_offset + _limit,
            )
        )
    return paths
