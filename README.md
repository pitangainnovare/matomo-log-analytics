# Matomo Server Log Analytics

This application is part of the new SciELO Usage Countage Solution. 
In our context the matomo-log-analytics represents an expanded and adapted version of the script import_logs.py, originally avaliable at the matomo-log-analytics repository.
We conceived and implemented this expanded version to permit a major capacity of error handling during the process of importing logs. 
Moreover, the application is capable of summarize each log imported in a way that is possible to automatize the process of retries, i.e., the process of import a log again.

## Requirements

* Python 2.7
* Matomo 3.14.1


## Data Structure

There are three database tables that control the data flow during the import of logs, as follows: (a) `LogFile`, (b) `LogFileSummary`, and (c) `LogFileDateStatus`. 
`LogFile` is a table that contains information with regard to the files to be imported in the Matomo. 
This table is composed of, for example, file's fullpath, name and status.
`Status` is a column that controls which file must be imported in Matomo. 
Since a file is imported, the application changes its status to a value that correspond to the success or not of the importing process. 
This value could be `partial`, `completed` or remains `queue`, that is its initial value. 
It will be `partial` when a file have failed to be imported completely. It will be `completed` when a log file have succeded to be imported fully.

`LogFileSummary` contains information with regard the summary generated during a process of import. 
It concentrates information with regard to the number of lines imported, ignored and so on. 
The number of requests done by bots or to static resources are also persisted. 

The last table is called `LogFileDateStatus`. This is the table that serves a process that extract pretables from the Matomo Raw Logs Tables.
`LogFileDateStatus` keeps information regarding all the files related to a day. The column status indicates when a day have or have not all its related log files imported (loaded) in Matomo.
Since a day have all its related files imported in Matomo, it is possible to extract what we call `Pretable`. This is a file that serves an application named SciELO Counter Usage.
Finally, the matomo-log-analytcs SciELO affects the four initial status. The other values (5 and 6) are controled by another application.


__Table 1.__ Status and description of the six steps related to the process of import and extract information from a set of log files.

| Step | Status | Description |
| --- | ------ | ----------- |
| 1 | Queue | It is necessary to import all the logs related to a day |
| 2 | Partial | At least one of the logs related to a day were imported |
| 3 | Loaded | All log files related to a day were imported successufully |
| 4 | Pretable | The pretable of a day was already extracted |
| 5 | Calculated | The COUNTER metrics related to a day were computed |
| 6 | Done | The Counter metrics related to a day were saved in the SUSHI Database Tables |
