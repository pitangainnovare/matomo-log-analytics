This application is part of the new SciELO Usage Countage Solution (for experimental purposes). In our context, the "matomo-log-analytics" represents an expanded and adapted version of the script import_logs.py, originally available at the matomo-log-analytics repository.
We conceived and implemented this expanded version to permit a major capacity of error handling during the process of importing logs.
Moreover, the application is capable of extract summarizing information of each log imported. With this data, we could automatize the process of reimporting log files, if needed.

## Requirements

* Python 2.7
* Matomo 3.14.1

## Data Structure

There are three tables in a database called control. These three tables are responsible for controlling the data flow during the importing of logs, as follows: (a) `cointrol_log_file`, (b) `control_log_file_summary`, and (c) `control_date_status`. 