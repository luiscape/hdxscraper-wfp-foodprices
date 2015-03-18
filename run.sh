#!/bin/bash

# Running scraper
R/bin/Rscript tool/code/scraper.R

# Loading data into DataStore.
source venv/bin/activate
python tool/code/create-datastore.py 4146b89f-4014-4020-9006-69ed2371207e api_key