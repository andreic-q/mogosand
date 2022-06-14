from datetime import datetime, tzinfo, timezone
from pymongo import MongoClient
import os


# Get MongoDB credentials
MONGODB_URI = os.getenv("MONGODB_URI_FEDERATED")
client = MongoClient(MONGODB_URI)

db = client.get_database("biquit")
coll = db.get_collection("sellerHistTest")

print(coll.count_documents)

# # start_date = datetime(2020, 5, 1, 0, 0, 0)  # May 1st
# # end_date = datetime(2020, 6, 1, 0, 0, 0)  # June 1st

pipeline = [
    {
        '$match': {
            'datetime': {
                '$gte': datetime(2022, 6, 13, 0, 0, 0, tzinfo=timezone.utc), 
                '$lt': datetime(2022, 6, 14, 0, 0, 0, tzinfo=timezone.utc)
            }
        }
    }
    ,{ '$limit': 1}
    ,{
        '$out': {
            's3': {
                'bucket': 'mongo-atlas-export-test', 
                'region': 'eu-west-2', 
                'filename': 'sellerHistTestArchived_1record', 
                'format': {
                    'name': 'parquet', 
                    'maxFileSize': '100MiB',
                    'columnCompression': 'gzip'
                }
            }
        }
    }
    # ,{ "background" : true }
]

curs = coll.aggregate(pipeline)

# explain =coll.explain().aggregate(pipeline)
# print(explain)
print('Archive created!')
# for document in curs:
#     print(document)