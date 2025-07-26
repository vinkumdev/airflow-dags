#!/bin/bash

# Exit if anything fails
set -e

export AIRFLOW_HOME=$(pwd)/airflow_home
mkdir -p "$AIRFLOW_HOME"

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install Airflow (minimal extras)
AIRFLOW_VERSION=3.0.1
PYTHON_VERSION=$(python --version | cut -d " " -f 2 | cut -d "." -f -2)
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "$CONSTRAINT_URL"


AIRFLOW_HOME=$(pwd)/airflow_home airflow standalone