exports = async function () {

  
  const datalake = context.services.get("FedDB1");
  const coll = "sellerHistArchive";
  const jobLogs = await datalake.db("test").collection("logs");
  const sellerHist =await datalake.db("biquit").collection(coll);
  
  const results =await jobLogs.find({"status" : {"$exists":0}},{"_id": 1,  "start_date": 1 },{"$orderby": { "started_at" : -1 }})
        .toArray();
  console.log('Valid runs: '+ results.length );
  
  if (results.length == 1 ) {
        results.forEach( doc =>  {
          start_dt_string = doc.start_date; 
          run_id = doc._id;
          });
          console.log(new Date(start_dt_string) + ' ' + new Date(Date.now()) ) 
          
          if ((new Date(start_dt_string)) < (new Date(Date.now())) ) {
            console.log('Start building pipeline for run sellerHistoryArchiveS3Log._id: ' + run_id );
            const end_date  = getNextDay(new Date(start_dt_string));
            var   recon_date = splitDate(start_dt_string);
            const out_path  = recon_date.join('/');
            const out_file_name = recon_date[0]+recon_date[1]+recon_date[2];
            
            const pipeline = getPipeline(start_dt_string,
                                         end_date,
                                         coll,
                                         out_path,
                                         out_file_name);
                                         
            const log_res =await updateRunLogs(run_id, 'RUNNING', end_date, 'Running the pipeline' );
            // console.log('Run _id: ' + log_res._id + ' with status: ' + log_res.status + ' @ '+ log_res.updated_at );
        
            //execute the pipeline
            return sellerHist.aggregate(pipeline,{ allowDiskUse: true })
                .next()
                .then(aggPipeOutput => {
                    console.log('Aggregation Pipeline output: '+ aggPipeOutput);
                    const completed = updateRunLogs(run_id, 'COMPLETED', end_date, 'pipeline executed: '+ JSON.stringify(aggPipeOutput) );
                    //create next run 
                    const nextRun = createNewRun({
                                               "collection": "biquit.SellerHistoryArchive",
                                               "job": "trigger.Export_Seller_Hist_Daily",
                                               "start_date": end_date.toISOString().slice(0, 10),
                                               "notes": "run initiated from trigger",
                                               "run_created_at": new Date(Date.now()),
                                               "updated_at": new Date(Date.now())
                                               });
                    return JSON.stringify(aggPipeOutput)
                  })
                .catch(error  => {
                       console.log(`Error logged: ${error}`);
                       const err = updateRunLogs(run_id, 'ERROR', end_date, 'run ended with error: '+ error); 
                       throw error;
                       });
        } else {
            console.log(`Start date: ${start_dt_string} for run id: ${run_id} is in the future in sellerHistoryArchiveS3Log` );
            return(`No runs executed.Next run Start date is in the future`);
            }
  }
  else if (results.length > 1) {
    console.log(`Too many runs in the log run collection sellerHistoryArchiveS3Log`);
    return(`No runs executed.Too many valid runs.`);
  }
  else {
      console.log(`No valid runs in the log run collection sellerHistoryArchiveS3Log`);
      return(`No runs executed. No valid runs.`)
  }

}
// -------  Function declaration section ---------- 

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

function getPipeline(start_date, end_date, coll, out_path, out_file_name){
  
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
                                            ,out_file_name]
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
  return pipeline;
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