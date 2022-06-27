from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current



# Get MongoDB credentials
MONGODB_URI = os.getenv("MONGODB_URI_TEST")
client = MongoClient(MONGODB_URI)

db = client.test
result = client['test-offerPipeline']['offers'].aggregate([
    {       '$match': {            'rawPrice': {                '$gt': 7            }   }    }
   ,{
            "$out": {
               "s3": {
                  "bucket": "mongo-sellerhistoryarchive",
                  "region": "eu-west-2",
                  "filename": "testout",
                  "format": {
                        "name": "parquet",
                        "maxFileSize": "10GB",
                        "maxRowGroupSize": "100MB"
                  }
               }
            }
   }
])
print (db)