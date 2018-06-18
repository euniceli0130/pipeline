import json
import requests
import logging
import itertools
import smtplib
import numpy as np
import pandas as pd
from datetime import datetime
from email import encoders
from utils import dayLimits, dayParser, isIn, assemble, flattenRow, databasePath, sqlite, QueryBuilder, read_config
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase


class ExtractFromAPI(object):

    def __init__(self):
        self.config = read_config()
        self.baseUrl = self.config["base_url"]
        self.queryTypes = self.config["queryTypes"]

    def generalQuery(self, queryType, startDate = None):
        if queryType in self.queryTypes and queryType != 'events':
            response = requests.get(self.baseUrl + queryType)
            return json.loads(response.content.decode('utf-8'))[queryType]
        elif queryType == 'events':
            if  datetime.today() < datetime.strptime(startDate, '%Y-%m-%d'):
                logging.error("Start date is greater than today! Please re-start.")
            targetUrl = self.baseUrl + queryType + "?days=" + dayLimits(startDate)
            response = requests.get(targetUrl)
            return json.loads(response.content.decode('utf-8'))['events']
        else:
            logging.error("Available query types are " + ", ".join(self.queryTypes) + "!")

    def dailyEventQuery(self):
        response = requests.get(self.baseUrl + "events?days=1")
        return json.loads(response.content.decode('utf-8'))['events']


class DataTransform(object):

    def __init__(self, rawData, queryType):
        self.config = read_config()
        self.rawData = rawData
        self.columnNames = self.config[queryType]
        self.subColumnNames = self.config["subschemas"]
        self.eventSchema = self.config["eventschema"]



    def plainTransformation(self):
        listOfTuple = list(map(lambda x: tuple(map(lambda y: x[y], self.columnNames)), self.rawData))
        return pd.DataFrame(listOfTuple, columns = self.columnNames), listOfTuple

    def customizeTransformation(self, exclude = None, endDate = None):
        tfVec = np.array([isinstance(self.rawData[0][self.columnNames[i]], list) for i in range(len(self.columnNames))])
        listIndex = np.where(tfVec == True)
        nonlistIndex = np.where(tfVec == False)
        res = []
        for eachDict in self.rawData:
            nonListFields = list(map(lambda x: eachDict[self.columnNames[x]], list(nonlistIndex[0])))
            listFields = list(map(lambda x: assemble(eachDict[self.columnNames[x]], self.subColumnNames),
                                  list(listIndex[0])))
            combinedRaw = list(itertools.product(*listFields))
            res += [tuple(flattenRow(nonListFields, comb)) for comb in combinedRaw]
        rawDF = pd.DataFrame(res, columns = np.array(self.eventSchema))
        rawDF =rawDF[isIn(rawDF["categoryID"].values, self.config["filter"]["categoryID"])]
        if endDate != None:
            rawDF = rawDF[dayParser(rawDF['geoDate'].values, endDate)]
        if exclude != None:
            rawDF = rawDF.drop(exclude, axis = 1)
        return rawDF, list(map(tuple, rawDF.itertuples(index=False)))

class LoadToSQLite3(object):

    def __init__(self, dbName):
        self.dbPath = databasePath()
        self.dbName = dbName
        self.sq = sqlite(self.dbPath, self.dbName)
        self.sql = QueryBuilder()

    def LoadFactTable(self, listOfTuples):
        if len(listOfTuples) == 0:
            pass
        else:
            tabName = "EventTable"
            # create table if not exists
            self.sq.execute(self.sql.create(tabName))
            # insert listOfTuples
            for each in listOfTuples:
                try:
                    self.sq.execute(self.sql.insert(tabName, each))
                except:
                    logging.warning("This entry already exists in the table. Skip!")

    def loadDimensionTable(self, listOfTuples, tabName):
        self.sq.execute(self.sql.create(tabName))
        for each in listOfTuples:
            try:
                self.sq.execute(self.sql.insert(tabName, each))
            except:
                logging.warning("This entry already exists in the table. Skip!")

class sendEmail(object):

    def __init__(self, email_send):
        config = read_config()['emailInfo']
        self.smtp = config['smtp']
        self.portal = config['portal']
        self.email_send = email_send
        self.email_password = config["password"]
        self.msg = MIMEMultipart()
        self.msg['From'] = config["emailAdd"]
        self.msg['To'] = email_send
        self.msg['Subject'] = config["subject"]
        self.body1 = config["bodyWithAttachment"]
        self.body2 = config["bodyWithoutAttachment"]

    def  compileAndSend(self, df = None, dfSource = None, dfCate = None, fileName = None):
        hasAttachment = isinstance(df, pd.DataFrame)
        if hasAttachment:
            writer = pd.ExcelWriter(fileName, engine='xlsxwriter')
            df.to_excel(writer, sheet_name = 'Events', index = False)
            dfSource.to_excel(writer, sheet_name = 'Source', index = False)
            dfCate.to_excel(writer, sheet_name = 'Category', index = False)
            writer.save()
            self.msg.attach(MIMEText(self.body1, 'plain'))
        else:
            self.msg.attach(MIMEText(self.body2, 'plain'))
        part = MIMEBase('application', "octet-stream")
        if hasAttachment:
            part.set_payload(open(fileName, "rb").read())
            encoders.encode_base64(part)
        if isinstance(df, pd.DataFrame):
            header = 'attachment; filename = {fName}'.format(fName = fileName)
            part.add_header('Content-Disposition', header)
        self.msg.attach(part)
        text = self.msg.as_string()
        server = smtplib.SMTP(self.smtp, self.portal)
        server.starttls()
        server.login(self.msg['From'], self.email_password)
        server.sendmail(self.msg['From'], self.email_send, text)
        server.quit()
