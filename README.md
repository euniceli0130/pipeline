# pipeline
EONET Simple Pipeline

The main objective is:
1. Set up a database to hold event data from EONET event API
2. Compile data into a spreadsheet
3. Email the spreadsheet to an email address specified by user

Here is my set-up:
1. Download the full project and unzip on the desktop (or anywhere). I used windows 10 OS, linux system may cause some issues 
   due to  the path format, please adjust it accordingly.
2. sqlite3 database is used (for simplicity), please download the DB Viewer from: https://sqlitebrowser.org/;
3. python34 is used as an interpreter with required packages specified in requirements.txt, run the following command:
   pip install -r {path}/requirements.txt
   It's better to use anaconda3 so that all packages used in this project are avilable;

I implemented a series of ETL task classes for this project:

1. ExtractFromAPI class is responsiblie to fetch data from EONET API, I provided two modes, a generyQuery which supports 
   specific dates window and a dialyEventQuery;

2. DataTransform class is used to streamline json file to dataframe and list of tuples, i.e., flaten the file. Again, it has 
   a plain transformation for raw data from category API and source API, and a more involved customized transformation for 
   raw data from event API;

3. LoadToSQLite3 class is used to load data to SQL3 database for both factTable and dimensional table (will be discussed shortly);

4. sendEmail class supports sending email with attachment and plain email

For database design, I used a "toy" start-schema, where fact table is EventTable and dimensional tables are CategoryTable and SourceTable:

EventTable(("id", "categoryID", "sourceID", "geoDate", "geoType", "geoCoordinates"), "title", "description", "link",
             "sourceURL") 
CategoryTable(("CategoryID"), "title", "link", "description", "layers") CategoryID FK to EventTable
SourceTable(("SourceID"), "title", "link", "source) SourceID FK to EventTable

Notice in the event table, we omitted category title column as when can drill down to dimensional table to find more details. Apart from that, as same event can have different geo information (date, location), whenever, we encounter list of list, we flatten them out by repeating, we think it can make future query from database easier.

The program can be run in two modes with 
python {path}\main.py {startDate} {endDate} {send_to} {whichMode Backfill or Daily} {Want to update dimensional table}

1. BACKFILL: in urrent database, we already have results for 2018-5-15 to 2018-6-15 and two other dimensional table (up to date) by running:

   python {path} "2018-5-15" "2018-6-15" "euniceli0130@gmail.com" "Backfill" True

This will do ETL + send email with excel attachment.

2. DAILY: one can run

  python {path} "" "" "euniceli0130@gmail.com" "Daily" False 
  
  This will do ETL + send email notifying the task is done.

Some efforts:

1. parameterized SQL by jinja, although not all used, the template in QueryFolder contains select, drop, insert, update, drop 
   basic query clauses, which can be compiled with real data via my queryBuilder. I used it to load transformed data into 
   database and fetch dimensional data from sqlite3 db to excel sheeet;
   
2. Config file controls the parameterization of the program, I tried to abstract the program as much as possible, the config 
   controls the parameters used in program so that in case of modification, we only need to modify config files;
 
3. The progarm allows backfill for specific period not limited to x days from today, plus, it will not repeatly insert entries
   database, if already exists, by referential integrity, it won't be entered;

Some improvements:
 
 1. I used very simple logging componenet, which can be greatly improved;
 
 2. The whole ETL process is better to be controlled by workflow manager, such as luigi or airflow, so it gives
    sufficient information of the status of system and increase efficiency of each task;







