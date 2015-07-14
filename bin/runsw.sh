#!/bin/bash

#
# HDX API Key.
#
HDX_KEY="YOUR_API_KEY"

#
# Running script.
#
source venv/bin/activate
python tool/scripts/datastore/create_datastore.py $HDX_KEY tool/config/food_prices_schema.json tool/data/temp.csv
