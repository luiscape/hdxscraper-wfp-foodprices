#!/bin/bash

#
# HDX API Key.
#
HDX_KEY="YOUR_API_KEY"

#
# Running script.
#
source venv/bin/activate
python scripts/datastore/create_datastore.py $HDX_KEY config/food_prices_schema.json data/temp.csv
