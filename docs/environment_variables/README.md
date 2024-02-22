# About

nr-ops relies heavily on [Airflow's environment variable conventions](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html).
TODO - Add more details about the environment variables used in the project.

Here's the list of environment variables that are used in the project. Not all environment variables are necessary for all the DAGs.
* `ENVIRONMENT`
* `GOOGLE_APPLICATION_CREDENTIALS`
* TODO


# How to bulk export environment variables?
* Create a new file called `.env`
* Then paste the contents of the `.env-template` file into the `.env` file
* In the `.env` file, replace the values of the environment variables with the actual values
  * You only need to set the environment variables that are necessary for the nr-ops run
  * You can remove the rest of the environment variables or leave them be with their default values
  * You can refer to the documentation below to understand the purpose of each environment variable and what value to set for each environment variable
* Finally, run the following commands to export the environment variables from the `.env` file
```
# Source: https://stackoverflow.com/questions/66780525/exporting-an-env-file-with-values-containing-space
set -a
. .env
set +a
```

# ENVIRONMENT VARIABLE REFERENCE

## ENVVAR - ENVIRONMENT
* Name: `ENVIRONMENT`
* Description: An optional but useful envvar to distinguish between different environments like `dev`, `prod`, `local` etc.
* Operators that rely on this envvar: 
  * None
* How to set this envvar?
  * Part of the `.env-template` file


## ENVVAR - GOOGLE_APPLICATION_CREDENTIALS
* Name: `GOOGLE_APPLICATION_CREDENTIALS`
* Description: The path to the GCP service account key file.
* Operators that rely on this envvar:
  * TODO
* How to set this envvar?
  * Part of the `.env-template` file
* Where to get the contents of `/path/to/your/service-account-key.json`:
  * You can create a service account key file from the GCP console. 
  * You can also use the `gcloud` CLI to create a service account key file.
* Example contents of the service account key file:
```
{
  "type": "service_account",
  "project_id": "SOME_PROJECT_ID",
  "private_key_id": "SOME_PRIVATE_KEY_ID",
  "private_key": "SOME_PRIVATE_KEY",
  "client_email": "SOME_EMAIL_ID",
  "client_id": "SOME_CLIENT_ID",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "SOME_CERT_URL"
}
```


