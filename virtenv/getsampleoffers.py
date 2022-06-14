import os
from datetime import datetime
from pymongo import MongoClient


# Get MongoDB credentials
MONGODB_URI = os.getenv("MONGODB_URI_FEDERATED")

client = MongoClient(MONGODB_URI)
print(client.test)

db = client.get_database("test-offerPipeline")
coll = db.get_collection("offers")

# start_date = datetime(2020, 5, 1, 0, 0, 0)  # May 1st
# end_date = datetime(2020, 6, 1, 0, 0, 0)  # June 1st

pipeline = [
    {   '$match': {
            'rawPrice': {'$gt': 33 }}    },
    { '$limit': 10}
]

curs = coll.aggregate(pipeline)
for document in curs:
    print(document)