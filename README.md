# README currently WIP

## Usage

1. Clone this repository
2. Download the dataset from [this link](https://github.com/RoTak00/roro-analiza/releases/download/dataset-cleaned-1.0.0/data-cleaned.zip)
3. Unzip the data-cleaned.zip file

## EDA scripts

A set of scripts for exploratory data analysis + associated plots lie under the folder `eda`.

Since some of these scripts use the local `roro_module`, for example to parse the local dataset using the `RoRoParser` class, you can run them from the repo root level as follows:

- download dataset and unzip as described in Usage section: below examples assume unzipped dataset lies at root level under the name `data-cleaned`
- install dependencies: `pip install -r requirements.txt`
- run on Windows: `$env:PYTHONPATH = $PWD; python eda\plot_mattr.py --data-path 'data-cleaned'`
- run on Linux / macOS: `export PYTHONPATH=$PWD; python eda/plot_mattr.py --data-path 'data-cleaned'`

## Scraper scripts

A set for scripts for scraping Moldovan news websites can be found under `scraper`. Main directions for expansion were "raioane" not covered by the original dataset, which had data from about 10 out of 32. Expanded dataset with all other raioane for which data could be sample from raional councils / city hall websites.

Different scrapers were built for each website, but they all function similarly, saving data inside the data-cleaned dataset in the same format as other documents.

The only external dependency is `beautifulsoup4`: `pip install beautifulsoup4`

