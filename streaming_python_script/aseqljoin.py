import sys
import pymongo
import datetime

from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

mongoClient = MongoClient("localhost", 27017)
db = mongoClient["maize"]
aseqljoin = db["aseqljoin"]
format = "%a %b %d %H:%M:%S %Z %Y"




for line in sys.stdin:
   line = line.strip()
   records = line.split('\t')
   type1 = records[0]
   if type1 == '\N' or type1 == 'NULL':
      type1 = "APICALL"
   dateTime = records[1]
   if dateTime == '\N':
      dateTime = "NULL"
   if dateTime != 'NULL':
      dateTime = datetime.strptime(dateTime, format)
   endpoint = records[2]
   if endpoint == '\N':
      endpoint  = "NULL"
   isResponse = records[3]
   if isResponse == '\N' or isResponse == 'NULL':
      isResponse = "false"
   isResponse = bool(isResponse == "true")
   isRequest = records[4]
   if isRequest == '\N'or isRequest == 'NULL':
      isRequest = "false"
   isRequest = bool(isRequest == "true")
   isError = records[5]
   if isError == '\N' or isError == 'NULL':
      isError = "false"
   isError = bool(isError == "true")
   duration = records[6]
   if duration == '\N' or duration == 'NULL':
      duration = "0"
   duration = int(duration)
   buckets = records[7]
   if buckets == '\N' or buckets == 'NULL':
      buckets = "0"
   buckets = int(buckets)
   buckete = records[8]
   if buckete == '\N' or buckets == 'NULL':
      buckete = "0"
   buckete = int(buckete)
   id1 = records[9]
   if id1 == '\N':
      id1 = 'NULL'
   if id1 != "NULL":
      id1 =  ObjectId(id1)
   session = records[10]
   if session == '\N':
      session = 'NULL'
   if session != "NULL":
      session = ObjectId(session)
   isTimedout = records[11]
   if isTimedout == '\N' or isTimedout == 'NULL':
      isTimedout = 'false'
   isTimedout = bool(isTimedout == "true")
   isQual = records[12]
   if isQual == '\N' or isQual == 'NULL':
      isQual = 'false'
   isQual = bool(isQual == "true")
   qual = records[13]
   if qual == '\N':
      qual = 'NULL'
   if qual != "NULL":
      qual = ObjectId(qual)
   isPositiveQual = records[14]
   if isPositiveQual == '\N' or isPositiveQual == 'NULL':
      isPositiveQual = 'false'
   isPositiveQual = bool(isPositiveQual == 'true')
   userAgent = records[15]
   if userAgent == '\N' or userAgent == 'NULL':
      userAgent = '{}'
   length = len(userAgent)
   tmp = {}
   if length > 2:
      userAgent = userAgent[1:len(userAgent) - 1]
      userAgent = userAgent.split(',')
      for ele in userAgent:
         ele = ele.split(':')
         ele[0] = ele[0][1:len(ele[0]) - 1]
         ele[1] = ele[1][1:len(ele[1]) - 1]
         tmp[ele[0]] = ele[1]
   useAgent = tmp
   lead = records[16]
   if lead == '\N':
      lead = 'NULL'
   if lead != "NULL":
      lead = ObjectId(lead)
   visit = records[17]
   if visit == '\N':
      visit = 'NULL'
   if visit != "NULL":
      visit = ObjectId(visit)
   ordered = records[18]
   if ordered == '\N' or ordered == 'NULL':
      ordered = 'false'
   ordered = bool(ordered == 'true')
   order = records[19]
   if order == '\N':
      order = 'NULL'
   if order != 'NULL':
      order = ObjectId(order)
   agent = records[20]
   if agent == '\N' or agent == 'NULL':
      agent = '{}'
   length = len(agent)
   tmp = {}
   if length > 2:
      agent = agent[1:len(agent) - 1]
      agent = agent.split(',')
      for ele in agent:
         ele = ele.split(':')
         ele[0] = ele[0][1:len(ele[0]) - 1]
         ele[1] = ele[1][1:len(ele[1]) - 1]
         tmp[ele[0]] = ele[1]
   agent = tmp
   error = records[21]
   if error == '\N':
      error = 'NULL'
   if error != "NULL":
      error = ObjectId(error)
   errorType = records[22]
   if errorType == '\N':
      errorType = 'NULL'
   errorCode = records[23]
   if errorCode == '\N':
      errorCode = 'NULL'
   errorMessage = records[24]
   if errorMessage == '\N':
      errorMessage = 'NULL'
   providerCode = records[25]
   if providerCode == '\N':
      providerCode = 'NULL'
   providerName = records[26]
   if providerName == '\N':
      providerName = 'NULL'
   severity = records[27]
   if severity == '\N' or severity == 'NULL':
      severity = '0'
   severity = int(severity)
   wasSuccessful = records[28]
   if wasSuccessful == '\N' or wasSuccessful == 'NULL':
      wasSuccessful = 'false'
   wasSuccessful = bool(wasSuccessful == "true")
   leadSource = records[29]
   if leadSource == '\N':
      leadSource = 'NULL'
   leadSourceName = records[30]
   if leadSourceName == '\N':
      leadSourceName = 'NULL'
   semTracking = records[31]
   if semTracking == '\N' or semTracking == 'NULL':
      semTracking  =  "{}"
   length = len(semTracking)
   tmp = {}
   if length > 2:
      semTracking = semTracking[1:len(semTracking) - 1]
      semTracking.split(',')
      for ele in semTracking: 
         ele.split(':')
         ele[0] = ele[0][1:len(ele[0]) - 1]
         ele[1] = ele[1][1:len(ele[1]) - 1]
         tmp[ele[0]] = ele[1]
   semTracking = tmp

   doc = {"type":type1,
                     "dateTime":dateTime,
                     "endpoint":endpoint,
                     "isResponse":isResponse,
                     "isRequest":isRequest,
                     "isError":isError,
                     "duration":duration,
                     "buckets":buckets,
                     "buckete":buckete,
                     "_id":id1,
                     "session":session,
                     "isTimedout":isTimedout,
                     "isQual":isQual,
                     "qual":qual,
                     "isPositiveQual":isPositiveQual,
                     "userAgent":useAgent,
                     "lead":lead,
                     "visit":visit,
                     "ordered":ordered,
                     "order":order,
                     "agent":agent,
                     "error":error,
                     "errorType":errorType,
                     "errorCode":errorCode,
                     "errorMessage":errorMessage,
                     "providerCode":providerCode,
                     "providerName":providerName,
                     "severity":severity,
                     "wasSuccessful":wasSuccessful,
                     "leadSource":leadSource,
                     "leadSourceName":leadSourceName,
                     "semTracking":semTracking,
   } 
   aseqljoin.insert(doc)
   


      
   
  

     



