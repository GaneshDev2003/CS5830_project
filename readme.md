# This repository contains code for the course project of CS5830: Big Data Laboratory

## This contains 3 sections

1. Data Fetch pipeline using Apache Airflow and Beam
2. Model building pipeline using MLflow
3. Dockerized FastAPI server and monitoring with Prometheus and Grafana

## Steps to reproduce the data fetch pipeline

1. Install Apache airflow
2. Place the `data_fetch_pipeline.py` file into `~/airflow/dags` directory
3. Run `airflow standalone` from the terminal
4. Search for the `data_fetch_pipeline` dag and trigger it

## Steps to run the dockerized FastAPI server

1. Change into the `fastapi` directory.
2. To build the image, run `docker build -t fastapi_image .`
3. Run `docker run -d --name fastapicontainer -p 80:80 fastapi_image`
4. Visit `localhost:80/docs` to view the FastAPI Swagger UI
