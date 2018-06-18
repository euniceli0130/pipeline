import logging
import sys
import pandas as pd
from utils import read_config, sqlite, databasePath, QueryBuilder
from Tasks import ExtractFromAPI, DataTransform, LoadToSQLite3, sendEmail


if __name__ == "__main__":
    startDate = str(sys.argv[1])
    endDate = str(sys.argv[2])
    email_send = str(sys.argv[3])
    Option = str(sys.argv[4])
    updateDimensionalTable = bool(sys.argv[5])

    config = read_config()
    dbName = config["databaseInfo"]["name"]

    if updateDimensionalTable == True:
        extractTask = ExtractFromAPI()
        taskSource = DataTransform(extractTask.generalQuery("sources"), "sources")
        taskCate = DataTransform(extractTask.generalQuery("categories"), "categories")
        dfSource, tupleSource = taskSource.plainTransformation()
        dfCate, tupleCate = taskCate.plainTransformation()
        loadTask = LoadToSQLite3(dbName)
        loadTask.loadDimensionTable(tupleSource, "SourceTable")
        loadTask.loadDimensionTable(tupleCate, "CategoryTable")

    if Option == "Backfill":
        '''
            BACK FILL JOB (ETL)
        '''
        extractTask = ExtractFromAPI()
        rawData = extractTask.generalQuery("events", startDate)
        transformTask = DataTransform(rawData, queryType ="events")
        dfEvents, TupleEvents = transformTask.customizeTransformation(["categoryTitle"], endDate)
        loadTask = LoadToSQLite3(dbName)
        loadTask.LoadFactTable(TupleEvents)
        '''
            Extract from DB  SEND EMAIL (To CLIENT)
        '''
        sq = sqlite(databasePath(), dbName)
        sql = QueryBuilder()
        tabName1 = "SourceTable"
        tabName2 = "CategoryTable"
        sq.execute(sql.select(tabName1, ['*']))
        resSource = sq.fetch(type = "many")
        sourceDF = pd.DataFrame(resSource, columns = config["sources"])
        sq.execute(sql.select(tabName2, ['*']))
        resCate = sq.fetch(type = "many")
        cateDF = pd.DataFrame(resCate, columns = config['categories'])
        FileName = "Back_Fill_FROM_" + startDate + "_TO_" + endDate + ".xlsx"
        sendEmailTask = sendEmail(email_send)
        sendEmailTask.compileAndSend(dfEvents, sourceDF, cateDF, FileName)

    elif Option == "Daily":
        extractTask = ExtractFromAPI()
        rawData = extractTask.dailyEventQuery()
        transformTask = DataTransform(rawData, queryType ="events")
        dfEvents, TupleEvents = transformTask.customizeTransformation(["categoryTitle"])
        loadTask = LoadToSQLite3(dbName)
        loadTask.LoadFactTable(TupleEvents)
        sendEmailTask = sendEmail(email_send)
        sendEmailTask.compileAndSend()
    else:
        logging.error("No Such Option, Please choose either Daily or Backfill!")



