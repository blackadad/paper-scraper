# paper-scraper

A proof of concept to scrape papers from journals

## Install

```bash
pip install git+https://github.com/blackadad/paper-scraper.git
```

## Usage

```python
papers = paperscraper.search_papers('bayesian model selection',
                                    limit=10,
                                    pdir='downloaded-papers')
```

## Improving Performance

If you can think of a website that can turn a DOI into a full-text PDF, you can set it via an environment variable:

```bash
export DOI2PDF='????'
```

## Note

Programmatically downloading papers is a grey area. Please use this package responsibly.
