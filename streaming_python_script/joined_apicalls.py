import sys
import pymongo
import datetime

from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

mongoClient = MongoClient("localhost", 27017)
db = mongoClient["maize"]
joined_apicalls = db["joined_apicalls"]
format = "%a %b %d %H:%M:%S %Z %Y"


for line in sys.stdin:
   line = line.strip();
   records = line.split('\t')
   type1 = records[0]
   if type1 == '\N' or type1 == 'NULL':
      type1 = "APICALL" 
   dateTime = records[1]
   if dateTime == '\N':
      dateTime = "NULL"
   if dateTime != "NULL": 
      dateTime = datetime.strptime(dateTime, format)
   endpoint = records[2]
   if endpoint == '\N':
      endpoint = "NULL"
   isResponse = records[3]
   if isResponse == '\N' or isResponse == 'NULL':
      isResposne = "false"
   isResponse = bool(isResponse == 'true')
   isRequest = records[4]
   if isRequest == '\N' or isRequest == 'NULL':
      isRequest = "false" 
   isRequest = bool(isRequest == 'true')
   isError = records[5]
   if isError == '\N' or isError == 'NULL':
     isError = "false"
   isError = bool(isError == 'true')
   duration = records[6]
   if duration == '\N' or duration == 'NULL':
      duration = '0'
   duration = int(duration)
   buckets = records[7]
   if buckets == '\N' or buckets == 'NULL':
      buckets = '0'
   buckets = int(buckets)
   buckete = records[8]
   if buckete == '\N' or buckete == 'NULL':
      buckete = '0'
   buckete = int(buckete)
   id1 = records[9]
   if id1 == '\N':
      id1 = "NULL"
   if id1 != 'NULL':
      id1 = ObjectId(id1)
   session = records[10]
   if session == '\N':
      session = "NULL"
   if session != "NULL":
      session = ObjectId(session)
   isTimedout = records[11]
   if isTimedout == '\N' or isTimedout == "NULL":
      isTimedout = 'false'
   isTimedout = bool(isTimedout == 'true')
   isQual = records[12]
   if isQual == '\N' or isQual == 'NULL':
      isQual = "false"
   isQual = bool(isQual == 'true')
   qual = records[13]
   if qual == '\N':
      qual = "NULL"
   if qual != "NULL":
      qual = ObjectId(qual)
   isPositiveQual = records[14]
   if isPositiveQual == '\N' or isPositiveQual == 'NULL':
      isPositiveQual = 'false'
   isPositiveQual = bool(isPositiveQual == 'true')
   lead = records[15]
   if lead == '\N':
      lead = "NULL"
   if lead != "NULL":
      lead = ObjectId(lead)
   ordered = records[16]
   if ordered == '\N' or ordered == 'NULL':
      ordered = 'false'
   ordered = bool(ordered == 'true')
   order = records[17]
   if order == '\N':
      order = "NULL"
   if order != "NULL":
      order = ObjectId(order)
   joined_apicall = {"type":type1,
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
                     "lead":lead,
                     "ordered":ordered,
                     "order":order,
   } 
   joined_apicalls.insert(joined_apicall)

      
   
  

     



