from datetime import datetime, tzinfo, timezone,timedelta
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
S3FED_MONGODB_URI = os.getenv("MONGODB_URI_SCIENCERD") 
BIQUIT_MONGODB_URI = os.getenv("MONGODB_URI_BQ") 

def countMongoDocs(connectionString, db, collection, pipeline):
    with MongoClient(connectionString) as client:
        # print(client.test)
        coll = client.get_database(db).get_collection(collection)
        cursor = coll.aggregate(pipeline, allowDiskUse=True )

        for results in cursor:
            print(f'{results["id"]} in {db}.{collection} collection between {start_date} and {end_date}')
        #return cursor      

start_date  = datetime(2022, 6, 25, 0, 0, 0, tzinfo=timezone.utc)
end_date    = datetime(2022, 6, 26, 0, 0, 0, tzinfo=timezone.utc) 

pipeline = [
            {
                '$match': {
                    'datetime': {
                        '$gte': start_date , 
                        '$lt':  end_date
                    }
                }
            } ,
            {  '$count': 'id'}
        ]

count_federated_sellerHist_docs = countMongoDocs(connectionString= S3FED_MONGODB_URI, db="s3Science" , collection="sellerHistArchive",pipeline=pipeline)
count_biquit_sellerHist_docs = countMongoDocs(connectionString=BIQUIT_MONGODB_URI, db="dataRetrieval" , collection="sellerHistoryArchive", pipeline=pipeline)
