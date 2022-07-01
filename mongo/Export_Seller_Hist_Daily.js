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
          console.log('sellerHistoryArchiveS3Log._id: ' + run_id );
          });
  }
  else {
      console.log(`No valid runs in the log run collection sellerHistoryArchiveS3Log`);
  }

  const end_date = getNextDay(new Date(start_dt_string))
  var recon_date = splitDate(start_dt_string);
  const out_path=recon_date.join('/');
  const outFileName = recon_date[0]+recon_date[1]+recon_date[2];
  console.log('Building Pipe for new run date:' + out_path );

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
                'bucket': 'mongo-sellerhistoryarchive', 
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

  const log_res =await updateRunLogs(run_id, 'RUNNING', end_date, 'Running the pipeline' );
  // console.log('Run _id: ' + log_res._id + ' with status: ' + log_res.status + ' @ '+ log_res.updated_at );
  
  //execute the pipeline
  return sellerHist.aggregate(pipeline,{ allowDiskUse: true })
      .next()
      .then(aggPipeOutput => {
          console.log('Aggregation Pipeline output: '+ aggPipeOutput);
          const completed = updateRunLogs(run_id, 'COMPLETED', end_date, 'pipeline executed: '+ JSON.stringify(aggPipeOutput) );
          // console.log('Run _id: ' + completed._id + ' with status: ' + completed.status + ' @ '+ completed.updated_at );
          return JSON.stringify(aggPipeOutput)
        })
      .catch(error  => {
             console.log(`Error logged: ${error}`);
             const err = updateRunLogs(run_id, 'ERROR', end_date, 'run ended with error: '+ error); 
            // console.log('Run _id: ' + err._id + ' with status: ' + err.status + ' @ '+ err.updated_at );
             throw error;
             });
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

    const client = context.services.get("qogita-test");
    const logs = await client.db("dataRetrieval").collection("sellerHistoryArchiveS3Log");

    if(status != 'COMPLETED'){
      const result = await logs.updateOne({"_id": run_id}, {"$set": {"run_started_at": new Date(Date.now())
                                                  ,"status"    : status 
                                                  ,"updated_at": new Date(Date.now())
                                                  ,"notes": notes}
                                          })
      console.log(`${result.modifiedCount} sellerHistoryArchiveS3Log document(s) was/were updated to status ${status}.`); 
      console.log(JSON.stringify(result));
        }
    else{
      const result = await logs.updateOne({"_id": run_id}, {"$set": {"end_date" : end_date.toISOString().slice(0, 10)
                                                    ,"status"    : status 
                                                    ,"updated_at": new Date(Date.now())
                                                    ,"notes": notes}
                                          })
      console.log(`${result.modifiedCount} sellerHistoryArchiveS3Log run document(s) was/were updated to status ${status}.`);  
      console.log(JSON.stringify(result));
      
    }
    // const log_result = await logs.findOne({'id':run_id});
}

async function createNewRun(newRun){
  const client = context.services.get("qogita-test");
  const result    = await client.db("dataRetrieval").collection("sellerHistoryArchiveS3Log").insertOne(newRun);
  console.log(`New run created with the following id: ${result.insertedId} and start date: ${newRun.start_date}`);
}