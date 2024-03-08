from setuptools import setup

exec(open("paperscraper/version.py").read())  # noqa: S102, SIM115

with open("README.md", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="paper-scraper",
    version=__version__,  # noqa: F821
    description="LLM Chain for answering questions from docs ",
    author="blackadad",
    author_email="hello@futureforecasts.io",
    url="https://github.com/blackadad/paper-scraper",
    license="GPLv3",
    packages=["paperscraper"],
    package_data={"paperscraper": ["py.typed"]},
    install_requires=["aiohttp", "pybtex", "pypdf"],
    test_suite="tests",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
)
