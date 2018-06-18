import dateutil.parser
import json
import sqlite3
import numpy as np
from os import path
from datetime import datetime
from functools import reduce
from jinja2 import Template

def basepath():
    return path.realpath(path.dirname(path.realpath(__file__))) + '\\'

def databasePath():
    return basepath() + "Database\\"

def queryPath():
    return basepath() + "QueryFolder\\"

def read_config():
    with open(basepath() + "Config\\config.json", "r") as file:
        data = json.load(file)
    return data

# count days between today and specific date
def dayLimits(date):
    currentTime = datetime.today() - datetime.strptime(date, '%Y-%m-%d')
    return str(currentTime.days + 1)

# compare UTC format datetimestr with date with format YYYY-mm-dd
def dayParser(dateTimeStrUTC, dateTimeStr):
    dateTimeStrVec = [dateTimeStr] * len(dateTimeStrUTC)
    return list(map(lambda x, y: datetime.strptime(dateutil.parser.parse(x).
                strftime('%Y-%m-%d'), '%Y-%m-%d') <= datetime.strptime(y, '%Y-%m-%d'),
                dateTimeStrUTC, dateTimeStrVec))

def isIn(field,targetSet):
    targetSets = [targetSet] * len(field)
    return list(map(lambda x, y: x in y, field, targetSets))

def assemble(listDict, subSchemas):
    keys = list(listDict[0].keys())
    for each in list(subSchemas.values()):
        if set(keys) == set(each):
            break
    return list(map(lambda y: list(map(lambda x: str(y[x]), each)), listDict))

def flattenRow(nonListIndex, comb):
    return nonListIndex + reduce(lambda x,y: x + y, comb)


class QueryBuilder(object):

    def help(self, templateName, variablesName = None):
        self.templatePath = queryPath() + "template\\" + templateName + '.sql'
        if variablesName != None:
            self.variablePath = queryPath() + "variables\\" + variablesName + ".json"
        with open(self.templatePath) as template:
            self.rawTemplate = template.read()
        template.close()

    def create(self, tableName):
        self.help("create", "schemas")
        with open(self.variablePath) as variable:
            rawVariables = json.load(variable)
        index = np.where(np.array([k for d in rawVariables for k in d.keys()]) == tableName)[0][0]
        variableVec = [k for d in rawVariables for k in d.values()]
        query = Template(self.rawTemplate).render(tableName = tableName, variables = variableVec[index])
        return query

    def insert(self, tableName, insertValuesVec):
        self.help("insert")
        return Template(self.rawTemplate).render(tableName = tableName, variables = insertValuesVec[:-1],
                                                 lastEntry = insertValuesVec[-1])

    def update(self, tableName, keyToBeUpdated, valuesToBeUpdated, criterionKey, criterionValue):
        self.help("update")
        keyToBeUpdated = [(keyToBeUpdated[i], valuesToBeUpdated[i]) for i in range(len(keyToBeUpdated))]
        criterion = [(criterionKey[j], criterionValue[j]) for j in range(len(criterionKey))]
        return Template(self.rawTemplate).render(tableName = tableName, toBeUpdated = keyToBeUpdated,
                                                 Criterion = criterion)

    def select(self, tableName, selectedItems, criterionKey = None, criterionValue = None):
        self.help("select")
        if criterionKey == None and criterionValue == None:
            criterionKey, criterionValue = [1], [1]
        criterion = [(criterionKey[j], criterionValue[j]) for j in range(len(criterionKey))]
        return Template(self.rawTemplate).render(tableName = tableName, selectedItem = selectedItems,
                        Criterion = criterion)

    def drop(self, tableName):
        self.help("drop")
        return Template(self.rawTemplate).render(tableName = tableName)

    def customizeQuery(self, templateName):
        self.help(templateName)
        return Template(self.rawTemplate).render()


class sqlite(object):
    def __init__(self, dbPath, dbName):
        self.conn = sqlite3.connect(dbPath + dbName + ".db")
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.conn.close()

    def execute(self, query):
        self.cursor.execute(query)
        self.conn.commit()

    def fetch(self, type, size = 0):
        if type == "one":
            temp = self.cursor.fetchone()
            return temp
        elif type == "many":
            temp = self.cursor.fetchmany(size)
            return temp
        else:
            temp = self.cursor.fetchall()
            return temp


