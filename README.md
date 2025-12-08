# README currently WIP

## Usage

1. Clone this repository
2. Download the dataset from [this link](https://github.com/RoTak00/roro-analiza/releases/download/dataset-cleaned-1.0.0/data-cleaned.zip)
3. Unzip the data-cleaned.zip file

## EDA scripts

A set of scripts for exploratory data analysis + associated plots lie under the folder `eda`.

Since some of these scripts use the local `roro_module`, for example to parse the local dataset using the `RoRoParser` class, you can run them from the repo root level as follows:

- install dependencies: `pip install -r eda/requirements.txt`
- run on Windows: `$env:PYTHONPATH = $PWD python eda\plot_mattr.py --data-path 'data-cleaned'`
- run on Linux / macOS: `export PYTHONPATH=$PWD; python eda/plot_mattr.py --data-path 'data-cleaned'`