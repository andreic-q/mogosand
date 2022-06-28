from datetime import datetime, tzinfo, timezone,timedelta
from webbrowser import BackgroundBrowser
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import json

def nextDatetime(start_dt):
    next_day = start_dt + timedelta(days=1)
    return next_day
# Load environment variables from .env
load_dotenv()

# Get MongoDB credentials
collection_name = "sellerHistArchive"
MONGODB_URI = os.getenv("MONGODB_URI_FEDERATED")
with MongoClient(MONGODB_URI) as client:
    debug_logs = {} 
    db = client.get_database("biquit")
    coll = db.get_collection(collection_name)
    # print (client.test)

    start_date  = datetime(2022, 6, 26, 0, 0, 0, tzinfo=timezone.utc)
    end_date    = datetime(2022, 6, 28, 0, 0, 0, tzinfo=timezone.utc) 
    
    debug_logs['jobRun'] = {'jobCreate': datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
                                , 'startDate': start_date.strftime("%Y-%m-%d")
                                , 'endDate' : end_date.strftime("%Y-%m-%d") }     
    debug_logs['dailyRun'] =[]

    while (nextDatetime(start_date).date()<=datetime.now(timezone.utc).date()) and (start_date.date()< end_date.date()): 
        out_path =  collection_name + "/" + str(start_date.strftime("%Y/%m/%d")) +"/" +str(start_date.strftime("%Y%m%d"))   # build the bucket prefix for each day
        run_end_date = nextDatetime(start_date)  # set the end date for the aggregation pipeline
        debug_start_msg= str(datetime.now(timezone.utc))+': Started archiving in s3://' + out_path + ' documents for : ' + str(start_date.strftime("%Y/%m/%d"))
        run_logs={}
        run_logs['msg'] ={'startMsg':debug_start_msg}
        print(debug_start_msg)

        pipeline = [
            {
                '$match': {
                    'datetime': {
                        '$gte': start_date , 
                        '$lt':  run_end_date
                    }
                }
            }  
            # ,{'$limit' : 10}
            ,{
                '$out': {
                    's3': {
                        'bucket': 'mongo-atlas-export-test', 
                        'region': 'eu-west-2', 
                        'filename': out_path ,
                        'format': {
                            'name': 'parquet', 
                            'maxFileSize': '2GB',
                            'columnCompression': 'gzip'
                        }
                    }
                }
            }
            # ,{ "background" : true }
        ]

        # curs = coll.aggregate(pipeline, allowDiskUse=True )
        # curs.close()
        debug_end_msg= str(datetime.now(timezone.utc))+': Finished archiving documents for : ' + str(start_date.strftime("%Y/%m/%d"))        
        print(debug_end_msg)
        run_logs['msg'].update({'endMsg': debug_end_msg})
        debug_logs['dailyRun'].append(run_logs['msg'])
        ### increment the date for the next run
        start_date =  nextDatetime(start_date)
        ### log the finished run in the log run table
        ### insert the next run in the log run table with no 'status' column
    debug_logs['jobRun']['jobEnd'] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    print(json.dumps(debug_logs))
    
    # client.close()

    ## find more about your pipeline
    # explain =coll.explain().aggregate(pipeline)
    # print(explain)