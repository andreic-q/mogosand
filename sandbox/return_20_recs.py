import os
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

# Get MongoDB credentials

load_dotenv()
MONGODB_URI = os.getenv("MONGODB_URI_BQ")
client = MongoClient(MONGODB_URI)

db = client.test
print(db)
# Get MongoDB credentials
db = client.get_database("dataRetrieval")
coll = db.get_collection("sellerHistoryTest")


pipeline = [
   { '$limit': 20}
    # ,{
    #     '$out': {
    #         's3': {
    #             'bucket': 'mongo-atlas-export-test', 
    #             'region': 'eu-west-2', 
    #             'filename': 'mmrt_20_recs', 
    #             'format': {
    #                 'name': 'json', 
    #                 'maxFileSize': '10MiB'
    #             }
    #         }
    #     }
    # }
]

curs = coll.aggregate(pipeline)
for document in curs:
    print(document)