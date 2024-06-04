from __future__ import annotations

import asyncio
import contextlib
import os
import tempfile
import time
from functools import partial
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from aiohttp import ClientResponseError
from pybtex.database import parse_string

import paperscraper
from paperscraper.exceptions import (
    CitationConversionError,
    DOINotFoundError,
    NoPDFLinkError,
)
from paperscraper.headers import get_header
from paperscraper.lib import (
    GOOGLE_SEARCH_MAX_PAGE_SIZE,
    RateLimits,
    clean_upbibtex,
    doi_to_bibtex,
    format_bibtex,
    openaccess_scraper,
    parse_google_scholar_metadata,
    reconcile_doi,
)
from paperscraper.utils import (
    ThrottledClientSession,
    encode_id,
    find_doi,
    search_pdf_link,
)


class TestThrottledClientSession(IsolatedAsyncioTestCase):
    async def test_throttling(self) -> None:

        async def get(session_: aiohttp.ClientSession) -> None:
            async with session_.get(
                "http://example.com", timeout=aiohttp.ClientTimeout(3.0)
            ) as response:
                response.raise_for_status()
                await response.text()

        tic = time.perf_counter()
        async with ThrottledClientSession() as session:
            await asyncio.gather(*(get(session) for _ in range(6)))
        assert time.perf_counter() - tic < 1, "Expected no throttling"

        tic = time.perf_counter()
        async with ThrottledClientSession(rate_limit=2) as session:
            await asyncio.gather(*(get(session) for _ in range(6)))
        assert 2.5 <= time.perf_counter() - tic <= 4.0, "Expected throttling"

    async def test_can_timeout(self) -> None:
        for rate_limit in (None, 1):
            async with ThrottledClientSession(rate_limit=rate_limit) as session:
                tic = time.perf_counter()
                try:
                    async with session.get(
                        # This URL should always timeout
                        "http://example.com:81",
                        timeout=aiohttp.ClientTimeout(3.0),
                    ):
                        pass
                except asyncio.TimeoutError:
                    toc = time.perf_counter()
                    assert 3.0 <= toc - tic <= 5.0, "Expected timeout"
                else:
                    raise AssertionError(
                        f"Should have timed out with rate limit {rate_limit}."
                    )

    async def test_empty_session(self) -> None:
        """Check an empty session doesn't crash us."""
        async with ThrottledClientSession(rate_limit=30.0):
            pass

    async def test_service_limit(self) -> None:
        async with ThrottledClientSession(rate_limit=10.0) as session:
            with (
                patch.object(
                    aiohttp.ClientSession,
                    "_request",
                    wraps=partial(aiohttp.ClientSession._request, session),
                ) as mock_request,
                pytest.raises(RuntimeError, match="service limit"),
            ):
                await session.get(
                    "http://httpbin.org/status/429", headers={"accept": "text/plain"}
                )
        assert (
            mock_request.call_count == 6
        ), "Expected first attempt followed by 5 retries"


class TestCrossref(IsolatedAsyncioTestCase):
    async def test_reconcile_dois(self) -> None:
        session = ThrottledClientSession(
            headers=get_header(), rate_limit=RateLimits.FALLBACK_SLOW.value
        )
        doi = "10.1056/nejmoa2200674"

        bibtex = await doi_to_bibtex(doi, session)
        assert bibtex

        # get title
        title = bibtex.split("title={")[1].split("},")[0]
        assert await reconcile_doi(title, [], session) == doi

        # format
        key = bibtex.split("{")[1].split(",")[0]
        assert format_bibtex(bibtex, key, clean=False)

    async def test_hard_reconciles(self):
        test_parameters: list[dict] = [
            {
                "title": (
                    "High-throughput screening of human genetic variants by pooled"
                    " prime editing."
                ),
                "doi": "10.1101/2024.04.01.587366",
            },
            {
                "title": (
                    "High-throughput screening of human genetic variants by pooled"
                    " prime editing."
                ),
                "authors": ["garbage", "authors", "that"],
                "doi": "10.1101/2024.04.01.587366",
            },
            {
                "title": (
                    "High throughput screening of human genetic variants by pooled"
                    " prime editing"
                ),
                "doi": "10.1101/2024.04.01.587366",
            },
        ]
        session = ThrottledClientSession(headers=get_header(), rate_limit=15 / 60)
        for test in test_parameters:
            assert await reconcile_doi(test["title"], [], session) == test["doi"]


def test_find_doi() -> None:
    test_parameters = [
        ("", None),
        ("https://www.sciencedirect.com/science/article/pii/S001046551930373X", None),
        ("https://www.academia.edu/download/110406132/2.pdf", None),
        ("https://doi.org/10.1056/nejmoa2200674", "10.1056/nejmoa2200674"),
        (
            "https://www.biorxiv.org/content/10.1101/2024.01.31.578268v1",
            "10.1101/2024.01.31.578268v1",
        ),
        (
            "https://www.biorxiv.org/content/10.1101/2024.01.31.578268v1.full-text",
            "10.1101/2024.01.31.578268v1",
        ),
        (
            "https://www.taylorfrancis.com/chapters/edit/10.1201/9781003240037-2/impact-covid-vaccination-globe-using-data-analytics-pawan-whig-arun-velu-rahul-reddy-pavika-sharma",
            "10.1201/9781003240037-2",
        ),
        (
            "https://iopscience.iop.org/article/10.7567/1882-0786/ab5c44/meta",
            "10.7567/1882-0786/ab5c44",
        ),
        (
            "https://iopscience.iop.org/article/10.7567/abc123abc/meta",
            "10.7567/abc123abc",
        ),
        (
            "https://iopscience.iop.org/article/10.7567/abc123abc.pdf",
            "10.7567/abc123abc",
        ),
        (
            "https://dx.doi.org/10.1016/j.arth.2005.04.023",
            "10.1016/j.arth.2005.04.023",
        ),
        ("https://doi.org/10.48550/arXiv.2401.00044", "10.48550/arXiv.2401.00044"),
        (
            "https://doi.org/10.26434/chemrxiv-2023-fw8n4-v3",
            "10.26434/chemrxiv-2023-fw8n4-v3",
        ),
        (
            "https://www.biorxiv.org/content/10.1101/2022.08.05.502972.full.pdf",
            "10.1101/2022.08.05.502972",
        ),
        (
            "https://doi.org/10.1002/(SICI)1097-0177(200006)218:2%3C235::AID-DVDY2%3E3.0.CO;2-G",
            "10.1002/(SICI)1097-0177(200006)218:2<235::AID-DVDY2>3.0.CO;2-G",
        ),
        (
            "https://anatomypubs.onlinelibrary.wiley.com/doi/10.1002/(SICI)1097-0177(200006)218:2%3C235::AID-DVDY2%3E3.0.CO;2-G",
            "10.1002/(SICI)1097-0177(200006)218:2<235::AID-DVDY2>3.0.CO;2-G",
        ),
        ("https://doi.org/10.1093/nar/gkae222", "10.1093/nar/gkae222"),
        (
            "https://doi.org/10.1007/s13592-019-00684-x?wt_mc=internal.event.1.sem.articleauthoronlinefirst&utm_source=articleauthoronlinefirst&utm_medium=email&utm_content=aa_en_06082018&articleauthoronlinefirst_20190913&fbclid=iwar2ipcqh8tqoyocdb2ryt-rqf2slmyf3s4k5_qwonmipan9_nqc_wiuabhi",
            "10.1007/s13592-019-00684-x",
        ),
    ]
    for link, expected in test_parameters:
        if expected is None:
            assert find_doi(link) is None
        else:
            assert find_doi(link) == expected


def test_encode_id() -> None:
    assert (
        encode_id("10.1056/nejmoa2119451")
        == encode_id("10.1056/NEJMOA2119451")
        == "945f1f30b11bcae6"
    )


def test_format_bibtex_badkey():
    bibtex1 = """
            @article{Moreira2022Safety,
            title        = {Safety and Efficacy of a Third Dose of BNT162b2 Covid-19 Vaccine},
            volume       = {386},
            ISSN         = {1533-4406},
            url          = {http://dx.doi.org/10.1056/nejmoa2200674},
            DOI          = {10.1056/nejmoa2200674},
            number       = {20},
            journal      = {New England Journal of Medicine},
            publisher    = {Massachusetts Medical Society},
            author       = {Moreira, Edson D. and Kitchin, Nicholas and Xu, Xia and Dychter, Samuel S. and Lockhart, Stephen and Gurtman, Alejandra and Perez, John L. and Zerbini, Cristiano and Dever, Michael E. and Jennings, Timothy W. and Brandon, Donald M. and Cannon, Kevin D. and Koren, Michael J. and Denham, Douglas S. and Berhe, Mezgebe and Fitz-Patrick, David and Hammitt, Laura L. and Klein, Nicola P. and Nell, Haylene and Keep, Georgina and Wang, Xingbin and Koury, Kenneth and Swanson, Kena A. and Cooper, David and Lu, Claire and Türeci, Özlem and Lagkadinou, Eleni and Tresnan, Dina B. and Dormitzer, Philip R. and Şahin, Uğur and Gruber, William C. and Jansen, Kathrin U.},
            year         = {2022},
            month        = may,
            pages        = {1910-1921}
            }
            """  # noqa: E501
    assert format_bibtex(bibtex1, "Moreira2022Safety", clean=False)


class Test0(IsolatedAsyncioTestCase):
    async def test_google_search_papers(self) -> None:
        for query, year, limit in [
            ("molecular dynamics", "2019-2023", 5),
            ("molecular dynamics", "2020", 5),
            ("covid vaccination", None, 10),
        ]:
            with self.subTest():
                papers = await paperscraper.a_search_papers(
                    query, search_type="google", year=year, limit=limit
                )
                assert len(papers) >= 3

    async def test_with_multiple_google_search_pages(self) -> None:
        papers = await paperscraper.a_search_papers(
            "molecular dynamics",
            search_type="google",
            year="2019-2023",
            limit=int(2.1 * GOOGLE_SEARCH_MAX_PAGE_SIZE),
        )
        assert len(papers) > GOOGLE_SEARCH_MAX_PAGE_SIZE


class TestGSearch(IsolatedAsyncioTestCase):
    async def test_gsearch(self):
        query = "molecular dynamics"
        papers = await paperscraper.a_gsearch_papers(query, year="2019-2023", limit=3)
        assert len(papers) >= 3

        # check their details
        for paper in papers.values():
            assert paper["citation"]
            assert paper["key"]
            assert paper["url"]
            assert paper["year"]
            assert paper["paperId"]
            assert paper["citationCount"]
            assert paper["title"]

    async def test_with_multiple_google_search_pages(self) -> None:
        papers = await paperscraper.a_gsearch_papers(
            "molecular dynamics", year="2019-2023", limit=5, _limit=2
        )
        assert len(papers) >= 5

    async def test_no_link_doesnt_crash_us(self) -> None:
        await paperscraper.a_gsearch_papers(
            "OAG-BERT: Pre-train Heterogeneous Entity-augmented Academic Language"
            " Models",
            year="2021",
        )

    async def test_no_doi_doesnt_crash_us(self) -> None:
        await paperscraper.a_gsearch_papers(
            "Letters to the American People, Part II (2019–2024)",  # noqa: RUF001
            year="2024",
        )


@pytest.mark.parametrize(
    ("title", "scraper_should_succeed"),
    [(
        "Simulation Intelligence: Towards a New Generation of Scientific Methods",
        "arxiv",
    )],
)
@pytest.mark.asyncio()
async def test_gsearch_examples(title, scraper_should_succeed):
    result = None

    async def status_callback(paper_title, scrape_result):
        # make sure most of words agree when lower
        t1 = set(title.lower().split())
        t2 = set(paper_title.lower().split())
        assert len(t1.intersection(t2)) / len(t1) > 0.5
        nonlocal result
        result = scrape_result

    scraper = paperscraper.default_scraper(callback=status_callback)

    # remove other scrapers
    for s in scraper.scrapers:
        if s.name != scraper_should_succeed:
            scraper.deregister_scraper(s.name)

    papers = await paperscraper.a_gsearch_papers(title, limit=1, scraper=scraper)
    assert len(papers) >= 1
    if result:
        assert scraper_should_succeed in result, "Scraper specified did not run"
        assert (
            result[scraper_should_succeed] == "success"
        ), f"Scraper {scraper_should_succeed} failed"
    else:
        raise AssertionError("No result from callback")


class Test1(IsolatedAsyncioTestCase):
    async def test_arxiv_to_pdf(self):
        arxiv_id = "1706.03762"
        path = "test.pdf"
        async with ThrottledClientSession(
            headers=get_header(), rate_limit=RateLimits.FALLBACK_SLOW.value
        ) as session:
            await paperscraper.arxiv_to_pdf(arxiv_id, path, session)
        assert paperscraper.check_pdf(path)
        os.remove(path)

    async def test_biorxiv_to_pdf(self):
        biorxiv_doi = "10.1101/2024.01.25.577217"
        path = "test.pdf"
        async with ThrottledClientSession(
            headers=get_header(), rate_limit=RateLimits.FALLBACK_SLOW.value
        ) as session:
            await paperscraper.xiv_to_pdf(biorxiv_doi, path, "www.biorxiv.org", session)
        assert paperscraper.check_pdf(path)
        os.remove(path)

    async def test_medrxiv_to_pdf(self):
        biorxiv_doi = "10.1101/2024.03.06.24303847"
        path = "test.pdf"
        async with ThrottledClientSession(
            headers=get_header(), rate_limit=RateLimits.FALLBACK_SLOW.value
        ) as session:
            await paperscraper.xiv_to_pdf(biorxiv_doi, path, "www.medrxiv.org", session)
        assert paperscraper.check_pdf(path)
        os.remove(path)

    async def test_pmc_to_pdf(self) -> None:
        with tempfile.NamedTemporaryFile() as tmpfile:
            for _ in range(3):  # Retrying on 403, pulling different header each retry
                async with ThrottledClientSession(
                    headers=get_header(), rate_limit=RateLimits.FALLBACK_SLOW.value
                ) as session:
                    cause_exc: Exception | None = None
                    try:
                        await paperscraper.pmc_to_pdf("8971931", tmpfile.name, session)
                    except RuntimeError as exc:
                        cause_exc = exc
                    else:
                        if paperscraper.check_pdf(tmpfile.name):
                            return
        raise AssertionError(
            "Failed to download and check PDF from PMC."
        ) from cause_exc

    def test_search_pdf_link(self) -> None:
        for url, expected in (
            ('<link rel="schema.DC" href="http://abc.org/DC/elements/1.0/" />', None),
            (
                '<a href="/doi/suppl/10.1010/spam.ham.0a0/some_file/abc_001.pdf" class="ext-link">PDF</a>',  # noqa: E501
                "/doi/suppl/10.1010/spam.ham.0a0/some_file/abc_001.pdf",
            ),
            (
                '<form method="POST" action="/deliver/fulltext/foo/71/1/spam-ham-123-456.pdf?itemId=%2Fcontent%2Fjournals%2F10.1010%2Fabc-def-012000-123&mimeType=pdf&containerItemId=content/journals/applesauce"\ntarget="/content/journals/10.1010/abc-def-012000-123-pdf" \ndata-title',  # noqa: E501
                None,
            ),
            (
                '<a href="#" class="fa fa-file-pdf-o access-options-icon"\nrole="button"><span class="sr-only">file format pdf download</span></a>',  # noqa: E501
                None,
            ),
        ):
            if isinstance(expected, str):
                assert search_pdf_link(url) == expected
            else:
                try:
                    search_pdf_link(url)
                except NoPDFLinkError:
                    pass
                else:
                    raise AssertionError("Should be unreachable")

    async def test_openaccess_scraper(self) -> None:
        assert not await openaccess_scraper(
            {"openAccessPdf": None}, MagicMock(), MagicMock()
        )

        mock_session = MagicMock()
        call_index = 0

        @contextlib.asynccontextmanager
        async def mock_session_get(*_, **__):
            mock_response = MagicMock(spec_set=aiohttp.ClientResponse)
            nonlocal call_index
            call_index += 1
            if call_index == 1:
                mock_response.text.side_effect = [
                    '<a class="suppl-anchor" href="/doi/suppl/10.1021/acs.nanolett.0c00513/suppl_file/nl0c00513_si_001.pdf">'  # noqa: E501
                ]
            else:
                mock_response.headers = {
                    "Content-Type": "application/pdf;charset=UTF-8"
                }
                mock_response.read.side_effect = [b"stub"]
            yield mock_response

        mock_session.get.side_effect = mock_session_get
        with tempfile.NamedTemporaryFile() as tmpfile:
            await openaccess_scraper(
                {
                    "openAccessPdf": {
                        "url": (
                            "https://pubs.acs.org/doi/abs/10.1021/acs.nanolett.0c00513"
                        )
                    }
                },
                tmpfile.name,
                mock_session,
            )

    async def test_pubmed_to_pdf(self) -> None:
        with tempfile.NamedTemporaryFile() as tmpfile:
            for _ in range(3):  # Retrying on 403, pulling different header each retry
                async with ThrottledClientSession(
                    headers=get_header(), rate_limit=RateLimits.FALLBACK_SLOW.value
                ) as session:
                    cause_exc: Exception | None = None
                    try:
                        await paperscraper.pubmed_to_pdf(
                            "27525504", tmpfile.name, session
                        )
                    except RuntimeError as exc:
                        cause_exc = exc
                    else:
                        if paperscraper.check_pdf(tmpfile.name):
                            return
        raise AssertionError(
            "Failed to download and check PDF from PubMed ID."
        ) from cause_exc

    async def test_link_to_pdf(self):
        link = "https://www.aclweb.org/anthology/N18-3011.pdf"
        path = "test.pdf"
        async with ThrottledClientSession(
            headers=get_header(), rate_limit=RateLimits.FALLBACK_SLOW.value
        ) as session:
            await paperscraper.link_to_pdf(link, path, session)
        assert paperscraper.check_pdf(path)
        os.remove(path)

    async def test_link2_to_pdf_that_can_raise_403(self) -> None:
        link = "https://journals.sagepub.com/doi/pdf/10.1177/1087057113498418"
        path = "test.pdf"
        try:
            async with ThrottledClientSession(
                headers=get_header(), rate_limit=RateLimits.FALLBACK_SLOW.value
            ) as session:
                await paperscraper.link_to_pdf(link, path, session)
            os.remove(path)

        except (RuntimeError, aiohttp.ClientResponseError) as e:
            assert "403" in str(e)  # noqa: PT017

    async def test_link3_to_pdf(self) -> None:
        with tempfile.NamedTemporaryFile() as tmpfile:
            for _ in range(3):  # Retrying
                async with ThrottledClientSession(
                    headers=get_header(), rate_limit=RateLimits.FALLBACK_SLOW.value
                ) as session:
                    await paperscraper.link_to_pdf(
                        "https://www.medrxiv.org/content/medrxiv/early/2020/03/23/2020.03.20.20040055.full.pdf",
                        tmpfile.name,
                        session,
                    )
                    if paperscraper.check_pdf(tmpfile.name):
                        return
        raise AssertionError("Failed to download and check PDF from medRxiv.")

    async def test_chemrxivlink_to_pdf(self) -> None:
        with tempfile.NamedTemporaryFile() as tmpfile:
            async with ThrottledClientSession(
                headers=get_header(), rate_limit=RateLimits.FALLBACK_SLOW.value
            ) as session:
                for _ in range(3):  # Retrying if invalid PDF download or 403 error
                    with contextlib.suppress(ClientResponseError):  # 403 error
                        await paperscraper.link_to_pdf(
                            "https://doi.org/10.26434/chemrxiv-2023-fw8n4",
                            tmpfile.name,
                            session,
                        )
                        if paperscraper.check_pdf(tmpfile.name):
                            return
                        # Download completed but PDF is invalid
        raise AssertionError("Failed to download and check PDF from ChemRxiv.")


class Test2(IsolatedAsyncioTestCase):
    async def test_search_papers(self):
        query = "molecular dynamics"
        papers = await paperscraper.a_search_papers(query, limit=1)
        assert len(papers) >= 1


class Test3(IsolatedAsyncioTestCase):
    async def test_search_papers_offset(self):
        query = "molecular dynamics"
        papers = await paperscraper.a_search_papers(query, limit=10, _limit=5)
        assert len(papers) >= 10


class Test4(IsolatedAsyncioTestCase):
    async def test_search_papers_plain(self):
        query = "meta-reinforcement learning meta reinforcement learning"
        papers = await paperscraper.a_search_papers(query, limit=3, verbose=True)
        assert len(papers) >= 3


class Test5(IsolatedAsyncioTestCase):

    async def test_search_papers_year(self) -> None:
        query = "covid vaccination"

        for year, name in [
            ("2019-2023", "normal range"),
            ("2023-2022", "flipped order"),
            (". 2021-2023", "discard upon bad formatting"),
        ]:
            with self.subTest(msg=name):
                papers = await paperscraper.a_search_papers(query, limit=1, year=year)
                assert len(papers) >= 1


class Test6(IsolatedAsyncioTestCase):
    async def test_verbose(self):
        query = "Fungi"
        papers = await paperscraper.a_search_papers(query, limit=1, verbose=False)
        assert len(papers) >= 1


class TestScraper(IsolatedAsyncioTestCase):
    async def test_custom_scraper(self) -> None:
        query = "covid vaccination"
        mock_scrape_fn = MagicMock()

        async def custom_scraper(paper, path, **kwargs):
            mock_scrape_fn(paper, path, **kwargs)

        scraper = paperscraper.Scraper()
        scraper.register_scraper(custom_scraper, priority=0, name="test", check=False)
        try:
            await paperscraper.a_search_papers(query, scraper=scraper)
        except RuntimeError as exc:
            assert (  # noqa: PT017
                exc.__cause__.status == 400  # type: ignore[union-attr]
            ), "Expected we should exhaust the search"

    async def test_scraper_callback(self) -> None:
        # make sure default scraper doesn't duplicate scrapers
        scraper = paperscraper.default_scraper()

        async def callback(paper, result):  # noqa: ARG001
            assert len(result) > 5

        scraper.callback = callback
        papers = await paperscraper.a_search_papers(  # noqa: F841
            "test", limit=1, scraper=scraper
        )
        await scraper.close()

    def test_scrape_default_timeout(self) -> None:
        os.environ.pop("PAPERSCRAPER_SCRAPE_FUNCTION_TIMEOUT", None)
        assert paperscraper.Scraper.SCRAPE_FUNCTION_TIMEOUT == 60.0

    async def test_scraper_timeout(self) -> None:
        os.environ.pop("PAPERSCRAPER_SCRAPE_FUNCTION_TIMEOUT", None)
        os.environ["USE_IN_MEMORY_CACHE"] = "true"
        scraper = paperscraper.Scraper()
        scraper.register_scraper(
            openaccess_scraper, attach_session=True, rate_limit=RateLimits.SCRAPER.value
        )
        tic = time.perf_counter()
        with tempfile.NamedTemporaryFile() as tmpfile:
            assert not await scraper.scrape(
                {
                    "title": (
                        "Martini 3: a general purpose force field for coarse-grained"
                        " molecular dynamics"
                    ),
                    "externalIds": {"DOI": "10.1038/s41592-021-01098-3"},
                    "openAccessPdf": {
                        # NOTE: swapped actual for non-routable IP address to avoid CI flakiness
                        "url": "https://10.255.255.1/test.pdf"
                    },
                },
                tmpfile.name,
            ), "Scrape was supposed to time out"
        assert (
            55.0 < time.perf_counter() - tic < 65.0
        ), "Expected test to be about 1-min"

    async def test_parser_runtime_error_doesnt_crash_us(self) -> None:
        mock_scraper = AsyncMock(name="stub", side_effect=[True])
        scraper = paperscraper.Scraper()
        scraper.register_scraper(mock_scraper, name="stub", check=False)
        with tempfile.TemporaryDirectory() as tmpdir:
            async with ThrottledClientSession(
                rate_limit=RateLimits.GOOGLE_SCHOLAR.value
            ) as session:
                assert not await scraper.batch_scrape(
                    papers=[{
                        "title": (
                            "An essential role of active site arginine residue in"
                            " iodide binding and histidine residue in electron"
                            " transfer for iodide oxidation by horseradish"
                            " peroxidase"
                        ),
                        "inline_links": {
                            "serpapi_cite_link": "https://serpapi.com/search.json?engine=google_scholar_cite&hl=en&q=uWOXVY5eGm8J",
                        },
                        "externalIds": {"DOI": "10.1023/A:1007154515475"},
                        "paperId": "stub",
                    }],
                    paper_file_dump_dir=tmpdir,
                    paper_parser=partial(
                        parse_google_scholar_metadata, session=session
                    ),
                ), "Expected empty return because this test checks for parser failure"
        mock_scraper.assert_awaited_once()


class Test8(IsolatedAsyncioTestCase):
    async def test_scraper_length(self):
        # make sure default scraper doesn't duplicate scrapers
        scraper = paperscraper.default_scraper()
        assert len(scraper.scrapers) == sum([len(s) for s in scraper.sorted_scrapers])


class Test10(IsolatedAsyncioTestCase):
    async def test_scraper_paper_search(self):
        # make sure default scraper doesn't duplicate scrapers
        papers = await paperscraper.a_search_papers(
            "649def34f8be52c8b66281af98ae884c09aef38b",
            limit=1,
            search_type="paper_recommendations",
        )
        assert len(papers) >= 1


class Test11(IsolatedAsyncioTestCase):
    async def test_scraper_doi_search(self):
        for _ in range(3):  # Retrying upon unsuccessful scrape
            papers = await paperscraper.a_search_papers(
                "10.1016/j.ccell.2021.11.002", limit=1, search_type="doi"
            )
            if len(papers) >= 1:
                return
        raise AssertionError("Failed to acquire a paper from DOI search.")


class Test12(IsolatedAsyncioTestCase):
    async def test_future_citation_search(self):
        # make sure default scraper doesn't duplicate scrapers
        papers = await paperscraper.a_search_papers(
            "649def34f8be52c8b66281af98ae884c09aef38b",
            limit=1,
            search_type="future_citations",
        )
        assert len(papers) >= 1


class Test13(IsolatedAsyncioTestCase):
    async def test_past_references_search(self):
        # make sure default scraper doesn't duplicate scrapers
        papers = await paperscraper.a_search_papers(
            "649def34f8be52c8b66281af98ae884c09aef38b",
            limit=1,
            search_type="past_references",
        )
        assert len(papers) >= 1


class Test14(IsolatedAsyncioTestCase):
    async def test_scraper_doi_search(self):
        try:
            papers = await paperscraper.a_search_papers(  # noqa: F841
                "10.23919/eusipco55093.2022.9909972", limit=1, search_type="doi"
            )
        except Exception as e:
            assert isinstance(e, DOINotFoundError)  # noqa: PT017


class Test15(IsolatedAsyncioTestCase):
    async def test_pdf_link_from_google(self):
        papers = await paperscraper.a_search_papers(
            "Multiplex Base Editing to Protect from CD33-Directed Therapy: Implications"
            " for Immune and Gene Therapy",
            limit=1,
            search_type="google",
        )
        assert len(papers) == 1


class Test16(IsolatedAsyncioTestCase):
    def test_format_bibtex(self) -> None:
        bibtex = """
            @['JournalArticle']{Salomón-Ferrer2013RoutineMM,
                author = {Romelia Salomón-Ferrer and A. Götz and D. Poole and S. Le Grand and R. Walker},
                booktitle = {Journal of Chemical Theory and Computation},
                journal = {Journal of chemical theory and computation},
                pages = {
                        3878-88
                        },
                title = {Routine Microsecond Molecular Dynamics Simulations with AMBER on GPUs. 2. Explicit Solvent Particle Mesh Ewald.},
                volume = {9 9},
                year = {2013}
            }
        """  # noqa: E501
        text = "Romelia Salomón-Ferrer, A. Götz, D. Poole, S. Le Grand, and R. Walker. Routine microsecond molecular dynamics simulations with amber on gpus. 2. explicit solvent particle mesh ewald. Journal of chemical theory and computation, 9 9:3878-88, 2013."  # noqa: E501
        assert paperscraper.format_bibtex(bibtex, "Salomón-Ferrer2013RoutineMM") == text

        bibtex2 = """
                @['Review']{Kianfar2019ComparisonAA,
            author = {E. Kianfar},
            booktitle = {Reviews in Inorganic Chemistry},
            journal = {Reviews in Inorganic Chemistry},
            pages = {157 - 177},
            title = {Comparison and assessment of zeolite catalysts performance dimethyl ether and light olefins production through methanol: a review},
            volume = {39},
            year = {2019}
            }
        """  # noqa: E501

        parse_string(clean_upbibtex(bibtex2), "bibtex")

        bibtex3 = """
        @None{Kianfar2019ComparisonAA,
            author = {E. Kianfar},
            booktitle = {Reviews in Inorganic Chemistry},
            journal = {Reviews in Inorganic Chemistry},
            pages = {157 - 177},
            title = {Comparison and assessment of zeolite catalysts performance dimethyl ether and light olefins production through methanol: a review},
            volume = {39},
            year = {2019}
        }
        """  # noqa: E501

        parse_string(clean_upbibtex(bibtex3), "bibtex")

        bibtex4 = """
        @['Review', 'JournalArticle', 'Some other stuff']{Kianfar2019ComparisonAA,
            author = {E. Kianfar},
            booktitle = {Reviews in Inorganic Chemistry},
            journal = {Reviews in Inorganic Chemistry},
            pages = {157 - 177},
            title = {Comparison and assessment of zeolite catalysts performance dimethyl ether and light olefins production through methanol: a review},
            volume = {39},
            year = {2019}
        }
        """  # noqa: E501

        parse_string(clean_upbibtex(bibtex4), "bibtex")

        bibtex5 = """
        @Review{Escobar2020BCGVP,
            author = {Luis E. Escobar and A. Molina-Cruz and C. Barillas-Mury},
            title = {BCG Vaccine Protection from Severe Coronavirus Disease 2019 (COVID19)},
            year = {2020}
        }
        """

        parse_string(clean_upbibtex(bibtex5), "bibtex")

        # Edge case where there is no title or author
        bibtex6 = """
        @article{2023,
            volume = {383},
            ISSN = {0378-4274},
            url = {http://dx.doi.org/10.1016/j.toxlet.2023.05.004},
            DOI = {10.1016/j.toxlet.2023.05.004},
            journal = {Toxicology Letters},
            publisher = {Elsevier BV},
            year = {2023},
            month = jul,
            pages = {33–42}
        }
        """  # noqa: RUF001
        key: str = bibtex6.split("{")[1].split(",")[0]
        # Check callers can intuit this conversion's failure
        with contextlib.suppress(CitationConversionError):
            format_bibtex(bibtex6, key, clean=False)

        # This BibTeX apparent has a trailing slash in its title
        bibtex7 = r"""
        @article{Jain2014Antioxidant,
            title={Antioxidant and Antibacterial Activities of Spondias pinnata Kurz. Leaves\},
            volume={4},
            ISSN={2231-0894},
            url={http://dx.doi.org/10.9734/ejmp/2014/7048},
            DOI={10.9734/ejmp/2014/7048},
            number={2},
            journal={European Journal of Medicinal Plants},
            publisher={Sciencedomain International},
            author={Jain, Preeti},
            year={2014},
            month=jan,
            pages={183–195}
        }
        """  # noqa: RUF001
        assert (
            "Antioxidant and Antibacterial Activities of Spondias pinnata"
            in format_bibtex(
                bibtex7, key=bibtex7.split("{")[1].split(",")[0], clean=False
            )
        )
