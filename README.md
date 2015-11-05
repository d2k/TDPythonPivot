# TDPythonPivot
Demo of the Teradata Python module: a process which implements a pivot function by generating SQL which does a pivot (rows to columns)

This version is supporting the RESTful API TD access. Using ODBC might require small changes.

## Install
clone the repository or 
download and unzip the zip file

## Configuration
The main configuration is done via three ini files:

* udaexec.ini - contains the main udaexec ini information 
* dwl/XXX.dwl - contains the database logon information 
* appini/YYYini - contains the specific pivot transformation information

### database logon information - dwl/demo.dwl
The database logoninformation requires the following parameter:

* production=True/False
* password=myPassword
* user=myUser
* system=mySystem
* host=myRestServer
* port=myRestServerPort - Standard 1080

### pivot configuration parameter
* DB 					= Databasename of the source table/view
* objectName 			= source table/view 
* groupbyVarList  		= ',' separated list of group by columns
* whereCondition  		= where condition for the pivot select. Typically constrains on period or subgroups etc. - free SQL text which can contain addition parameters which can be defined below 

* t0 					= optional, reference date for the period condition. Example: current_date-extract(day from current_date)
* numberOfPeriodsBack	= optional, numeric. Number of periods (days, weeks, months, years back. Need to make the whereCondition syntactically correct - see as an example demo.ini

* rangeQuery 			= SQL which will return the range which should be considered for the privot transformation - this doesn't need to be real ranges. It can also be names, product list etc. 
 
* rangeType 			= type of the rangeQuery result (Numeric or Char)
* rangeVar 				= varable which contains the range values

* denormVarInList 		= ',' separated list which contains the column names which should be denormalized
* denormVarOutList		= ',' separated list which contains the denormalized column names - the orig source column name is stored in the title info
* denormAggFunction 	= aggregatation function used for the pivot transformation - most common are MAX, MIN, SUM, AVG. rangeType Char should use MAX or MIN
* replaceNullValue		= Null replace value - Null is a valid value. Right now the solution can not handle different values in case denormVarInList contains a list of vars.

* materializeFlag 			= True/False, indicates that the transformed data should be stored into a table. In case of False the generated SQL will just printed to STDOUT. The following parameter are only relevant in case this one is True
* createTableFlag 			= True/False, indicates that a table should be created (True) or the target tables exists and the data is added to the table via Insert / Select (False)
* replaceExistingTableFlag 	= True/False, indicates that the table should be replaced in case it already exists
* materializeDB 			= target DB for the materialized table
* materializeTable 			= table name for the materialized table
* materializeComment 		= Comment String for the materialized table
* materializePI 			= full primary index statement for the materialized table. Example: Primary Index (${groupbyVarList}) 

### demo parameter settings
The application comes with 3 demo parameter settings:

* appini/demo1.ini 
contains the parameter to denormalize the day_of_week, day_of_calendar  columns of the sys_calendar.calendar table for the last 12 full months. 
The result is stored in a table.

* appini/demo2.ini
will denormalize the day_of_week column for the last 12 full months. The result columns will contain the number how often this weekday exists in the specific month. E.g. the demoralization contains an aggregation. 
The result is again stored in a table.

* appini/demo3.1.ini & appini/demo3.2.ini
will denormalize the columntype of the dbc.columns table. Again the columns contain the column count per columntype value.
demo3.1.ini will create a new table 
demo3.2.ini will add new data to the existing table. 

### program call 
example programm calls

python PivotGenerator.py -h 

result: 

usage: PivotGenerator.py [-h] [-l LOGONFILE] [-c CONFIGFILE]

Teradata Pivot SQL generator

optional arguments:
  -h, --help            show this help message and exit
  -l LOGONFILE, --logonfile LOGONFILE
                        logon file for the pivot operation
  -c CONFIGFILE, --configfile CONFIGFILE
                        config file for the pivot generation

python PivotGenerator.py -l dwl/demo.dwl  -c appini/demo1.ini 

