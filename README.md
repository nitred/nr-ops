# About

An opinionated operator framework.

# Pre-requisites

* `python=3.9`, next milestone will be `python=3.11` in Q1 2024
* IMPORTANT: We are now using `poetry==1.4.2`. `poetry` as a dependency been removed from dev dependencies in the `pyproject.toml` file and instead, it is expected to be installed manually using `pip install poetry==1.4.2` in a separate environment. Some dependency conflicts have been resolved by doing this.


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

# Other Important Notes

* If using any Google or GCP connectors (except GoogleAds) it's best to have the `GOOGLE_APPLICATION_CREDENTIALS` environment variable set to the path of your service account key file.
* If using Google Ads connectors, then set `GOOGLE_ADS_CONFIGURATION_FILE_PATH` environment variables to the path of your configuration/credentials file.
  ```
  developer_token: "ADS_DEVELOPER_TOKEN"
  json_key_file_path: "PATH_TO_SERVICE_ACCOUNT"
  login_customer_id: "GOOGLE_ADS_ID_WITHOUT_HYPHENS"
  impersonated_email: "ANY_REAL_GOOGLE_ADS_USER_WHO_HAS_ACCESS_TO_ADS_ACCOUNT"  
  use_proto_plus: true/false
  ```


# License

This repo uses the MIT license. See the LICENSE file for more details.
