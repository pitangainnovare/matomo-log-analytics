This application is part of a new SciELO Usage Countage Solution (for experimental purposes, at this moment). In our context, the "matomo-log-analytics" represents an expanded and adapted version of the script import_logs.py, originally available at the matomo-log-analytics repository. We conceived and implemented this expanded version to permit us to have a major capacity of error handling during the process of importing logs.
Moreover, the application is capable of extract summarizing information for each log imported. With this data, we could automatize the process of reimporting log files, if needed.


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
COLLECTION=scl 
COPY_FILES_LIMIT=10
DIR_PRETABLES=/app/data/pretables
DIR_SUMMARY=/app/data/summary 
DIR_WORKING_LOGS=/app/data/working
DIRS_USAGE_LOGS=/app/usage-logs/hiperion-logs,/app/usage-logs/node03-logs
LOAD_FILES_LIMIT=10
LOG_FILE_DATABASE_STRING=mysql://user:pass@localhost:3306/matomo
LOGGING_LEVEL=DEBUG
MATOMO_ID_SITE=1
MATOMO_API_TOKEN=e536004d5816c66e10e23a80fbd57911 
MATOMO_URL=http://172.17.0.4
MATOMO_RECORDERS=12
MAX_PRETABLE_DAYS=10
PYTHONHASHSEED=0
RETRY_DIFF_LINES=110000
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

__Extract pretables__

Execute the file _extract_pretables_ using a docker run proccess.

```shell
docker run --rm --env-file .env -v {HOST_DIR_LOGS}:/app/usage-logs -v {HOST_DIR_DATA}:/app/data scielo-matomo-manager extract_pretables
```

## Data Structure

There are three tables in a schema database called `control`. These tables are responsible for controlling the data flow during the importing of logs, as follows: (a) `control_log_file`, (b) `control_log_file_summary`, and (c) `control_date_status`.
