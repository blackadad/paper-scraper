import os
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock

from pybtex.database import parse_string

import paperscraper
from paperscraper.exceptions import DOINotFoundError
from paperscraper.headers import get_header
from paperscraper.lib import clean_upbibtex, openaccess_scraper
from paperscraper.utils import ThrottledClientSession


class Test0(IsolatedAsyncioTestCase):
    async def test_google_search_papers(self):
        query = "molecular dynamics"
        papers = await paperscraper.a_search_papers(
            query, search_type="google", year="2019-2023"
        )
        assert len(papers) >= 1

        query = "molecular dynamics"
        papers = await paperscraper.a_search_papers(
            query, search_type="google", year="2020"
        )
        assert len(papers) >= 1

        query = "covid vaccination"
        papers = await paperscraper.a_search_papers(query, search_type="google")
        assert len(papers) >= 1


class Test1(IsolatedAsyncioTestCase):
    async def test_arxiv_to_pdf(self):
        arxiv_id = "1706.03762"
        path = "test.pdf"
        async with ThrottledClientSession(
            headers=get_header(), rate_limit=15 / 60
        ) as session:
            await paperscraper.arxiv_to_pdf(arxiv_id, path, session)
        assert paperscraper.check_pdf(path)
        os.remove(path)

    async def test_biorxiv_to_pdf(self):
        biorxiv_doi = "10.1101/2024.01.25.577217"
        path = "test.pdf"
        async with ThrottledClientSession(
            headers=get_header(), rate_limit=15 / 60
        ) as session:
            await paperscraper.xiv_to_pdf(biorxiv_doi, path, "www.biorxiv.org", session)
        assert paperscraper.check_pdf(path)
        os.remove(path)

    async def test_medrxiv_to_pdf(self):
        biorxiv_doi = "10.1101/2024.03.06.24303847"
        path = "test.pdf"
        async with ThrottledClientSession(
            headers=get_header(), rate_limit=15 / 60
        ) as session:
            await paperscraper.xiv_to_pdf(biorxiv_doi, path, "www.medrxiv.org", session)
        assert paperscraper.check_pdf(path)
        os.remove(path)

    async def test_pmc_to_pdf(self):
        pmc_id = "8971931"
        path = "test.pdf"
        async with ThrottledClientSession(
            headers=get_header(), rate_limit=15 / 60
        ) as session:
            await paperscraper.pmc_to_pdf(pmc_id, path, session)
        assert paperscraper.check_pdf(path)
        os.remove(path)

    async def test_openaccess_scraper(self) -> None:
        assert not await openaccess_scraper(
            {"openAccessPdf": None}, MagicMock(), MagicMock()
        )

    async def test_pubmed_to_pdf(self):
        path = "test.pdf"
        async with ThrottledClientSession(
            headers=get_header(), rate_limit=15 / 60
        ) as session:
            await paperscraper.pubmed_to_pdf("27525504", path, session)
        assert paperscraper.check_pdf(path)
        os.remove(path)

    async def test_link_to_pdf(self):
        link = "https://www.aclweb.org/anthology/N18-3011.pdf"
        path = "test.pdf"
        async with ThrottledClientSession(
            headers=get_header(), rate_limit=15 / 60
        ) as session:
            await paperscraper.link_to_pdf(link, path, session)
        assert paperscraper.check_pdf(path)
        os.remove(path)

    async def test_link2_to_pdf_that_can_raise_403(self):
        link = "https://journals.sagepub.com/doi/pdf/10.1177/1087057113498418"
        path = "test.pdf"
        try:
            async with ThrottledClientSession(
                headers=get_header(), rate_limit=15 / 60
            ) as session:
                await paperscraper.link_to_pdf(link, path, session)
            os.remove(path)

        except RuntimeError as e:
            assert "403" in str(e)  # noqa: PT017

    async def test_link3_to_pdf(self):
        link = "https://www.medrxiv.org/content/medrxiv/early/2020/03/23/2020.03.20.20040055.full.pdf"
        path = "test.pdf"
        async with ThrottledClientSession(
            headers=get_header(), rate_limit=15 / 60
        ) as session:
            await paperscraper.link_to_pdf(link, path, session)
        assert paperscraper.check_pdf(path)
        os.remove(path)

    async def test_chemrxivlink_to_pdf(self):
        link = "https://doi.org/10.26434/chemrxiv-2023-fw8n4"
        path = "test.pdf"
        async with ThrottledClientSession(
            headers=get_header(), rate_limit=15 / 60
        ) as session:
            await paperscraper.link_to_pdf(link, path, session)
        assert paperscraper.check_pdf(path)
        os.remove(path)


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


class Test7(IsolatedAsyncioTestCase):
    async def test_custom_scraper(self):
        query = "covid vaccination"
        scraper = paperscraper.Scraper()
        scraper = scraper.register_scraper(
            lambda paper, path, **kwargs: None,  # noqa: ARG005
            priority=0,
            name="test",
            check=False,
        )
        papers = await paperscraper.a_search_papers(query, limit=5, scraper=scraper)
        assert len(papers) >= 5


class Test8(IsolatedAsyncioTestCase):
    async def test_scraper_length(self):
        # make sure default scraper doesn't duplicate scrapers
        scraper = paperscraper.default_scraper()
        assert len(scraper.scrapers) == sum([len(s) for s in scraper.sorted_scrapers])


class Test9(IsolatedAsyncioTestCase):
    async def test_scraper_callback(self):
        # make sure default scraper doesn't duplicate scrapers
        scraper = paperscraper.default_scraper()

        async def callback(paper, result):  # noqa: ARG001
            assert len(result) > 5
            print(result)

        scraper.callback = callback
        papers = await paperscraper.a_search_papers(  # noqa: F841
            "test", limit=1, scraper=scraper
        )
        await scraper.close()


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
        papers = await paperscraper.a_search_papers(
            "10.1016/j.ccell.2021.11.002", limit=1, search_type="doi"
        )
        assert len(papers) == 1


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
            "Multiplex Base Editing to Protect from CD33-Directed Therapy: Implications for Immune and Gene Therapy",  # noqa: E501
            limit=1,
            search_type="google",
        )
        assert len(papers) == 1


class Test16(IsolatedAsyncioTestCase):
    def test_format_bibtex(self):
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
