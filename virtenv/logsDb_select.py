from datetime import datetime, tzinfo, timezone
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI_TEST")
client = MongoClient(MONGODB_URI)
logsDb = client.get_database("dataRetrieval")
logsColl = logsDb.get_collection("sellerHistoryArchiveS3Log")
print (client.test)
# print(logsColl.count_documents({}))
curs_docs = logsColl.find()
for records in curs_docs:
    print(records)

def updateRunLogs(id, status, end_date='', notes=''):

    if(status != 'COMPLETED'):
        logsColl.update_one({"_id": id}, {"$set": {"run_started_at": datetime.utcnow() 
                                                ,"status"    : status 
                                                ,"updated_at": datetime.utcnow()
                                                ,"notes": notes}
                                        })
    else:
        logsColl.update_one({"_id": id}, {"$set": {"end_date" : end_date
                                                  ,"status"    : status 
                                                  ,"updated_at": datetime.utcnow()
                                                  ,"notes": notes}
                                        })


cursor = logsColl.find({"status" : {"$exists":0}},{"_id": 1,  "start_date": 1 })
# print(logsColl.count_documents({}))

if cursor:
    for doc in cursor:
        doc_id = doc["_id"]
else:
    print('Nothing to be seen here')
print(doc_id)

# logsColl.update_one({"_id": doc_id}, {"$set": {"modiffied_at": datetime.utcnow()}})
# logsColl.update_one({"_id": doc_id}, {"$set": {"test_status": "RUNNING"}})

# updateRunLogs(doc_id,'PENDING')

# start_date  = datetime(2022, 6, 13, 0, 0, 0, tzinfo=timezone.utc)

# curs = coll.aggregate(pipeline)

# explain =coll.explain().aggregate(pipeline)
# print(explain)
# print('Archive created!')
# for document in curs:
#     print(document)
