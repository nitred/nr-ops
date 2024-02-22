# About

An opinionated operator & ETL framework.

Documentation is work in progress.

![python_vs_nr_ops.png](docs%2Fimages%2Fpython_vs_nr_ops.png)

# Quick Start

## Quick Start Local

```
# Make sure you have python=3.9 installed
pip install nr-ops

# You can download the configs/mock_config.yaml file from the repo
nr-ops --config mock_config.yaml
```

## Quick Start Docker

### Using python:3.9 docker image 
```
# First clone the repo and cd into it

docker run -v $(pwd)/configs/mock_config.yaml:/mock_config.yaml python:3.9 /bin/bash -c "pip install nr-ops && nr-ops --config /mock_config.yaml"
```

### Using python:3.9 docker image explicitly for amd64 architecture
```
# First clone the repo and cd into it

docker run --platform linux/arm64 -v $(pwd)/configs/mock_config.yaml:/mock_config.yaml python:3.9 /bin/bash -c "pip install nr-ops && nr-ops --config /mock_config.yaml"
```

### Using nr-ops docker image
TODO


# Local Development Environment

In order to test nr-ops locally against a local development environment that includes both the nr-ops package as well as infrastructure dependencies like Airflow, minio (S3) etc you can follow the steps below.

* First create a `.env` file by copying the contents of the `.env-template` file and replacing the values of the environment variables with the actual values.
  * More instructions can be found here: [docs/environment_variables/README.md](docs/environment_variables/README.md)
* Make sure you have `python=3.9` installed (use environment manager of your choice)
  * Other python version may work but it's been tested with `python=3.9`
  * In order to use other versions, you will need to change the line in the `pyproject.toml` file that specifies the python version to use. For example if you'd like to use `python=3.11` then you will need to change the line `python = ">=3.9,<3.10"` to `python = ">=3.11,<3.12"`.
* Install poetry
```
pip install poetry==1.4.2
```
* Install nr-ops locally in development mode
```
poetry install
```
* Run all docker dependencies locally in a new terminal (or as a daemon if you prefer)
  * This installs all the necessary dependencies like Airflow, minio (S3) etc
 ```
 docker-compose --env-file .env -f docker/docker-compose-local.yml up
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
