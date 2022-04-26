This application is part of a new SciELO Usage Countage Solution.


## Requirements

* Python 2.7
* Matomo 3.14.1
* Docker


## Setting up environmental variables


## Install

1. /app/usage-logs (a directory that contains usage-logs files)
2. /app/data (a directory where all the resulting files will be stored)

You have to set up all the environmental variables (in the `.env` file), such as:

```
COLLECTION=collection_acronym
DIRS_USAGE_LOGS=/app/usage-logs/
LOG_FILE_DATABASE_STRING=mysql://user:pass@localhost:3306/database
LOGGING_LEVEL=DEBUG
```


## Run

__Update control_log_file table__

Execute the file _update_available_logs_ using a docker run proccess. 

```shell
docker run --rm --env-file .env -v {HOST_DIR_LOGS}:/app/usage-logs -v {HOST_DIR_DATA}:/app/data scielo-matomo-manager update_available_logs
```


__Load available logs__

Execute the file _load_logs_ using a docker run proccess.

```shell
docker run --rm --env-file .env -v {HOST_DIR_LOGS}:/app/usage-logs -v {HOST_DIR_DATA}:/app/data scielo-matomo-manager load_logs
```


## Data Structure

There are three tables in a schema database called `control`. These tables are responsible for controlling the data flow during the importing of logs, as follows: (a) `control_log_file`, (b) `control_log_file_summary`, and (c) `control_date_status`.
