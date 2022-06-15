from datetime import datetime, tzinfo, timezone
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Get MongoDB credentials
MONGODB_URI = os.getenv("MONGODB_URI_FEDERATED")
client = MongoClient(MONGODB_URI)


db = client.get_database("biquit")
coll = db.get_collection("sellerHistArchive")
print (client.test)


start_date  = datetime(2022, 6, 13, 0, 0, 0, tzinfo=timezone.utc)
end_date    = datetime(2022, 6, 14, 0, 0, 0, tzinfo=timezone.utc) 

pipeline = [
    {
        '$match': {
            'datetime': {
                '$gte': start_date , 
                '$lt': end_date 
            }
        }
    }
    # ,{'$limit': 10}
    ,{
        '$out': {
            's3': {
                'bucket': 'mongo-atlas-export-test', 
                'region': 'eu-west-2', 
                'filename': 'sellerHistArchive_14062022_full_500MiB', 
                'format': {
                    'name': 'parquet', 
                    'maxFileSize': '500MiB',
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
# print('Archive created!')
for document in curs:
    print(document)