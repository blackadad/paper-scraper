import paperscraper
import os


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


def test_arxiv_to_pdf():
    arxiv_id = "1703.10593"
    path = "test.pdf"
    paperscraper.arxiv_to_pdf(arxiv_id, path)
    assert os.path.exists(path)
    os.remove(path)


def test_pmc_to_pdf():
    pmc_id = "4122119"
    path = "test.pdf"
    paperscraper.pmc_to_pdf(pmc_id, path)
    assert os.path.exists(path)
    os.remove(path)


def test_doi_to_pdf():
    doi = "10.1021/acs.jctc.9b00202"
    path = "test.pdf"
    paperscraper.doi_to_pdf(doi, path)
    assert os.path.exists(path)
    os.remove(path)


def test_search_papers():
    query = "molecular dynamics"
    papers = paperscraper.search_papers(query, limit=1)
    assert len(papers) == 1
