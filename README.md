# About

An opinionated operator & ETL framework.

Documentation is work in progress.

![python_vs_nr_ops.png](docs%2Fimages%2Fpython_vs_nr_ops.png)


### Metadata

* Current stable version of nr-ops: `0.27.1.0`
* Current supported python versions: `3.9`
  * python versions `3.8`, `3.10` and `3.11` may work but haven't been tested
* Current poetry version used: `1.4.2`
* Current docker image version: `nitred/nr-ops:0.27.1.0-dev1`


### Why and how does nr-ops rely on Airflow?

*[Apache Airflow](https://github.com/apache/airflow) is a platform to programmatically author, schedule, and monitor workflows.* It sits at the core of most data-engineering workflows in many companies. It is mature and battle tested. Also, it comes pre-packaged with a lot of connectors to various data sources and sinks (think S3, GCS, BigQuery, Snowflake etc). It also has a rich ecosystem of plugins and operators that enable you to run your workflows on various engines like Kubernetes, ECS, Dask, Spark etc.

nr-ops was created primarily to simplify writing ETLs. It made sense to directly depend on the work done in Airflow. nr-ops has connectors which are often just light wrappers around Airflow's connectors. nr-ops uses [Airflow's environment variable conventions](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html) even if the underlying connectors have nothing to do with Airflow's own connectors. Why? Coming up with a custom convention just for nr-ops seemed pointless. Also if nr-ops were used in Airflow jobs, then nr-ops can easily use Airflow's connection management which significantly reduces operational work.

nr-ops can be used independently of Airflow. At the moment installing nr-ops also installs Airflow. This dependency will be made optional in the future.


# Quick Start

## Quick Start Local

```
# Make sure you are working in a python environemnt with python=3.9
pip install nr-ops

# You can download the configs/mock_config.yaml file from the repo
nr-ops --config mock_config.yaml
```

## Quick Start Docker

### Use nr-ops docker image from dockerhub
* Docker images for `linux/amd64` and `linux/arm64` are available on dockerhub. You can find the latest version of the image [here](https://hub.docker.com/r/nitred/nr-ops/tags?page=1&ordering=last_updated). At the moment, only `-dev` tags are available. The `-dev` tags are meant for development and testing purposes. This is because the Dockerfile used to build the image is not optimized for production use and also allows root access. The `-dev` tags will be removed once the image is optimized for production use.
* Run nr-ops using the docker image using the `mock_config.yaml` (which is already present in the docker image)
```
docker run nitred/nr-ops:0.27.1.0-dev1 bash -c "nr-ops --config ~/configs/mock_config.yaml"
```
* Run nr-ops using the docker image using a custom config file
```
docker run -v /path/to/your/config.yaml:/config.yaml nitred/nr-ops:0.27.1.0-dev1 bash -c "nr-ops --config /config.yaml"
```


### Build and use nr-ops docker image locally
* Build docker image locally
```
docker build -f docker/Dockerfile-prod -t nitred/nr-ops:local .
```
* Run nr-ops using the docker image using the `mock_config.yaml` (which is already present in the docker image)
```
docker run nitred/nr-ops:local bash -c "nr-ops --config ~/configs/mock_config.yaml"
```
* Run nr-ops using the docker image using a custom config file
```
docker run -v /path/to/your/config.yaml:/config.yaml nitred/nr-ops:local bash -c "nr-ops --config /config.yaml"
```


# Local Development Environment

In order to develop and test nr-ops locally against a local development environment that includes both the nr-ops package as well as infrastructure dependencies like Airflow, minio (S3) etc, you can follow the steps below.


## Install nr-ops

* Clone repo
```
git clone https://github.com/nitred/nr-ops
cd nr-ops
```
* Create and activate a new python environment with python=3.9. `conda` is recommended and it is best installed using miniconda. You can install the latest miniconda for your platform from [here](https://docs.anaconda.com/free/miniconda/). Once installed make sure `conda` is available in your PATH. You can check this by running `conda --version` in your terminal.
```
conda create -n firstvet-nr-ops python=3.9 -y
conda activate firstvet-nr-ops
``` 
* Install poetry. At the time of writing this, we are using poetry version 1.4.2. The version of poetry to use for a specific tag/branch can be found in the root README.md of the repo.
```
pip install poetry==1.4.2
```
* Install firstvet-nr-ops
```
poetry install
```
* Check if firstvet-nr-ops is installed correctly by running the following command
```
nr-ops --help
```

## (Optional) Setup infrastructure dependencies locally using docker 

* First create a `.env` file by copying the contents of the `.env-template` file. The values in the `.env-template` will work fine with the `docker/docker-compose-local  and replacing the values of the environment variables with the actual values. More instructions can be found here: [docs/environment_variables/README.md](docs/environment_variables/README.md). Here's the list of environment variables that are required by the docker-compose-local.yml file to work.

* Make sure you have `python3.9` installed (use environment manager of your choice)
  * Other python version may work but only `python3.9` has been tested
  * In order to use other versions of python, you will need to change a line in the `pyproject.toml` file that specifies the python version to use. For example if you'd like to use `python=3.11` then you will need to change the line `python = ">=3.9,<3.10"` to `python = ">=3.11,<3.12"`. After making this change, you can follow the steps below.
* Install poetry
```
pip install poetry==1.4.2
```
* Install nr-ops locally in development mode
```
poetry install
```
* Create a docker network 
```
docker network create --driver=bridge --subnet=172.100.100.128/25 nr-ops-network-n
```
* Run all docker dependencies locally in a new terminal (or as a daemon if you prefer)
  * This installs all the necessary dependencies like Airflow, minio (S3) etc
 ```
 docker-compose --env-file .env -f docker/docker-compose-local.yml up
 ```

# License

Copyright 2024 Nitish Reddy Koripalli

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
