# About

An opinionated operator framework.

# Pre-requisites

* `python=3.9`, next milestone will be `python=3.11` in Q1 2024
* `pip install poetry==1.1.15`


# Setup

* Run all docker dependencies locally in a new terminal (or as a daemon if you prefer)
   ```
   docker-compose --env-file .env -f docker/docker-compose-local.yml up
   ```


# Useful Commands

* Airflow CLI
    ```
    airflow config get-value database sql_alchemy_conn
    airflow config get-value core executor
    ```


# License

This repo uses the MIT license. See the LICENSE file for more details.
