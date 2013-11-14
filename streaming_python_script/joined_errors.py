import sys
import pymongo
import datetime

from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

mongoClient = MongoClient("localhost", 27017)
db = mongoClient["maize"]
joined_errors = db["joined_errors"]
format = "%a %b %d %H:%M:%S %Z %Y"

for line in sys.stdin:
   line = line.strip()
   records = line.split('\t')
   type1 = records[0]
   if type1 == '\N' or type1 == 'NULL':
      type1 = "ERROR"
   dateTime = records[1]
   if dateTime == '\N':
      dateTime = 'NULL'
   if dateTime != 'NULL':
      dateTime = datetime.strptime(dateTime, format)
   id1 = records[2]
   if id1 == '\N':
      id1 = "NULL"
   if id1 != "NULL":
      id1 = ObjectId(id1)
   session = records[3]
   if session == '\N':
      session = "NULL"
   if session != "NULL":
      session = ObjectId(session)
   errorType = records[4]
   if errorType == '\N':
      errorType = "NULL"
   errorCode = records[5]
   if errorCode == '\N':
      errorCode = "NULL"
   errorMessage = records[6]
   if errorMessage == '\N':
      errorMessage = "NULL"
   providerCode = records[7]
   if providerCode == '\N':
      providerCode = "NULL"
   providerName = records[8]
   if providerName == '\N':
      provdierName = "NULL"
   severity = records[9]
   if severity == '\N' or severity == 'NULL':
      severity = "0"
   severity = int(severity)
   apicall = records[10]
   if apicall == '\N':
      apicall = "NULL"
   if apicall != "NULL":
      apicall = ObjectId(apicall)
   lead = records[11]
   if lead == '\N':
      lead = 'NULL'
   if lead != "NULL":
      lead = ObjectId(lead)
   ordered = records[12]
   if ordered == '\N' or ordered == 'NULL':
      ordered = 'false'
   ordered = bool(ordered == 'true')
   order = records[13]
   if order == '\N' :
      order = 'NULL'
   if order != 'NULL':
      order = ObjectId(order)

   joined_error = {"type":type1,
                     "dateTime":dateTime,
                     "_id":id1,
                     "session":session,
                     "errorType":errorType,
                     "errorCode":errorCode,
                     "errorMessage":errorMessage,
                     "providerCode":providerCode,
                     "providerName":providerName,
                     "severity":severity,
                     "apicall":apicall,
                     "lead":lead,
                     "ordered":ordered,
                     "order":order,
   } 
   joined_errors.insert(joined_error)

      
   
  

     



