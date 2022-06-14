import os
from datetime import datetime
from pymongo import MongoClient

# Get MongoDB credentials
MONGODB_URI = os.getenv("MONGODB_URI_FEDERATED")
client = MongoClient(MONGODB_URI)

db = client.test
print(db)
# Get MongoDB credentials
db = client.get_database("dslaketest")
coll = db.get_collection("testColl")

# # start_date = datetime(2020, 5, 1, 0, 0, 0)  # May 1st
# # end_date = datetime(2020, 6, 1, 0, 0, 0)  # June 1st

pipeline = [
   { '$limit': 20}
    ,{
        '$out': {
            's3': {
                'bucket': 'mongo-atlas-export-test', 
                'region': 'eu-west-2', 
                'filename': 'mmrt_20_recs', 
                'format': {
                    'name': 'json', 
                    'maxFileSize': '10MiB'
                }
            }
        }
    }
]

curs = coll.aggregate(pipeline)
# for document in curs:
#     print(document)