import paperscraper
import os
from unittest import IsolatedAsyncioTestCase
from paperscraper.utils import ThrottledClientSession
from paperscraper.headers import get_header


def test_format_bibtex():
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
    """
    text = "Romelia Salomón-Ferrer, A. Götz, D. Poole, S. Le Grand, and R. Walker. Routine microsecond molecular dynamics simulations with amber on gpus. 2. explicit solvent particle mesh ewald. Journal of chemical theory and computation, 9 9:3878-88, 2013."
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
    """

    paperscraper.format_bibtex(bibtex2, "Kianfar2019ComparisonAA")

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
    """

    paperscraper.format_bibtex(bibtex3, "Kianfar2019ComparisonAA")

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
    """

    paperscraper.format_bibtex(bibtex3, "Kianfar2019ComparisonAA")


class Test(IsolatedAsyncioTestCase):
    async def test_arxiv_to_pdf(self):
        arxiv_id = "1703.10593"
        path = "test.pdf"
        async with ThrottledClientSession(
            headers=get_header(), rate_limit=15 / 60
        ) as session:
            await paperscraper.arxiv_to_pdf(arxiv_id, path, session)
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

    async def test_doi_to_pdf(self):
        doi = "10.1021/acs.jctc.9b00202"
        path = "test.pdf"
        async with ThrottledClientSession(
            headers=get_header(), rate_limit=15 / 60
        ) as session:
            await paperscraper.doi_to_pdf(doi, path, session)
        assert paperscraper.check_pdf(path)
        os.remove(path)

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

    async def test_link2_to_pdf(self):
        link = "https://journals.sagepub.com/doi/pdf/10.1177/1087057113498418"
        path = "test.pdf"
        async with ThrottledClientSession(
            headers=get_header(), rate_limit=15 / 60
        ) as session:
            await paperscraper.link_to_pdf(link, path, session)
        assert paperscraper.check_pdf(path)
        os.remove(path)

    async def test_search_papers(self):
        query = "molecular dynamics"
        papers = await paperscraper.a_search_papers(query, limit=1)
        assert len(papers) == 1

    async def test_search_papers_offset(self):
        query = "molecular dynamics"
        papers = await paperscraper.a_search_papers(query, limit=3, _limit=1)
        assert len(papers) == 3

    async def test_search_papers_plain(self):
        query = "meta-reinforcement learning meta reinforcement learning"
        papers = await paperscraper.a_search_papers(query, limit=1)
        assert len(papers) == 1


def test_search_papers_logger():
    query = "meta-reinforcement learning meta reinforcement learning"
    papers = paperscraper.search_papers(query, limit=1, verbose=True)
    assert len(papers) == 1
