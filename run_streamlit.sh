#!/usr/bin/env bash

echo "Starting 3PHI Streamlit app"

# load env
while IFS='=' read -r key value; do
    [[ -z "$key" || "$key" =~ ^# ]] && continue
    export "$key=$value"
done < .env

# activate venv
source .venv/bin/activate

# install deps
.venv/bin/python -m pip install -q -r requirements.txt

# run streamlit with correct python path
PYTHONPATH=$(pwd) streamlit run ui/app.py