exports = async function () {

  
  const datalake = context.services.get("FedDB1");
  const coll = "sellerHistArchive";
  const jobLogs = await datalake.db("test").collection("logs");
  const sellerHist =await datalake.db("biquit").collection(coll);
  
  const results =await jobLogs.find({"status" : {"$exists":0}},{"_id": 1,  "start_date": 1 },{"$orderby": { "started_at" : -1 }})
        .toArray();
  if (results.length > 0) {
      results.forEach( doc =>  {
          start_dt_string = doc.start_date; 
          run_id = doc._id;
          });
  }
  else {
      console.log(`No runs in the log run table`);
  }

  const end_date = getNextDay(new Date(start_dt_string))
  var recon_date = splitDate(start_dt_string);
  const out_path=recon_date.join('/');
  const outFileName = recon_date[0]+recon_date[1]+recon_date[2];
  console.log(out_path + "/" + outFileName);

  const pipeline = [
      {
        '$match': 
            {datetime: 
                    {
                    '$gte': new Date(start_dt_string),
                    '$lt':  end_date
                    }
            }
      }
      , {
        '$out': {
            's3': {
                'bucket': 'mongo-atlas-export-test', 
                'region': 'eu-west-2', 
                'filename': {'$concat':[ coll, "/"
                                          ,out_path
                                          ,"/"
                                          ,outFileName]
                            }, 
                'format': {
                    'name': 'parquet', 
                    'maxFileSize': '2GB',
                    'columnCompression': 'gzip'
                }
                ,"errorMode": "stop"
            }
        }
    }
    ];
try{
  const log_res =await updateRunLogs(run_id, 'RUNNING', end_date, 'pipeline: ' + pipeline );
  console.log('Run _id: ' + log_res._id + ' with status: ' + log_res.status + ' @ '+ log_res.updated_at );
  //execute the pipeline
  aggCursor = await sellerHist.aggregate(pipeline,{ allowDiskUse: true });
  
  // console.log(aggCursor);
  
  
  const completed =await updateRunLogs(run_id, 'COMPLETED', end_date, 'pipeline executed: '+ pipeline );
  console.log('Run _id: ' + completed._id + ' with status: ' + completed.status + ' @ '+ completed.updated_at );
  
  createNewRun({
                "collection": "biquit.SellerHistoryArchive",
                "job": "trigger.Export_Seller_Hist_Daily",
                "start_date": end_date.toISOString().slice(0, 10),
                "notes": "run initiated from trigger",
                "run_created_at": new Date(Date.now()),
                "updated_at": new Date(Date.now())
                })
  return aggCursor;
  
  } catch (error) {
    // if (error instanceof MongoServerError) {
      console.log(`Error logged: ${error}`); // special case for some reason
    // }
  const err =await updateRunLogs(run_id, 'ERROR', end_date, 'run ended in error'+ error);  
  console.log('Run _id: ' + err._id + ' with status: ' + err.status + ' @ '+ err.updated_at );
  throw error; // still want to crash
  } 
}

//  Sat Dec 31 2022
// console.log(getPreviousDay(new Date('2023-01-01')));
function getPreviousDay(date = new Date()) {
 const previous = new Date(date.getTime());
 previous.setDate(date.getDate() - 1);
 return previous;
}

// Mon  Jan 02 2023
// console.log(getNextDay(new Date('2023-01-01')));

function getNextDay(date = new Date()) {
const next = new Date(date.getTime());
next.setDate(date.getDate() + 1);
return next;
}

//split date into array ['YYYY','MM','DD']
function splitDate(date){
 var split_date = date.split('-');
 return split_date;
}

async function updateRunLogs(run_id, status, end_date = false, notes= false){
  // try {
    const client = context.services.get("qogita-test");
    const logs    = await client.db("dataRetrieval").collection("sellerHistoryArchiveS3Log");

    if(status != 'COMPLETED'){
      const result = await logs.updateOne({"_id": run_id}, {"$set": {"run_started_at": new Date(Date.now())
                                                  ,"status"    : status 
                                                  ,"updated_at": new Date(Date.now())
                                                  ,"notes": notes}
                                          })
      console.log(`${result.modifiedCount} Completed document(s) was/were updated to status ${status}.`);  
        }
    else{
      const result = await logs.updateOne({"_id": run_id}, {"$set": {"end_date" : end_date.toISOString().slice(0, 10)
                                                    ,"status"    : status 
                                                    ,"updated_at": new Date(Date.now())
                                                    ,"notes": notes}
                                          })
      console.log(`${result.modifiedCount} Other run document(s) was/were updated to status ${status}.`);  
    }
      
  // } finally{
    // console.log(`${result.modifiedCount} document(s) was/were updated.`);
    const log_result = await logs.findOne();
    return log_result;
  // }
}

/**
* Create a new Airbnb listing
* @param {MongoClient} client A MongoClient that is connected to a cluster with the sample_airbnb database
* @param {Object} newListing The new listing to be added
*/
async function createNewRun(newRun){
  const client = context.services.get("qogita-test");
  const result    = await client.db("dataRetrieval").collection("sellerHistoryArchiveS3Log").insertOne(newRun);
  console.log(`New run created with the following id: ${result.insertedId} and start date: ${newRun.start_date}`);
}