exports = async function () {

  const datalake = context.services.get("FedDB1");
  const coll = "sellerHistArchive";
  const jobLogs = datalake.db("test").collection("logs");
  const sellerHist = datalake.db("biquit").collection(coll);
  
  const result =await jobLogs.find({"status" : {"$exists":0}},{"_id": 1,  "start_date": 1 },{"$orderby": { "started_at" : -1 }})
        .toArray();
  result.forEach(doc =>  start_dt_string = doc.start_date);
  
  // console.log(start_dt_string);
  // console.log(new Date(start_dt_string));
  // const result =await logsColl.findOne({ "start_date": "2021-07-20"});
  // console.log(result._id);
   
  // nextRunDoc.forEach( function(docs) { print( "Next Run: " + docs.start_date ) } ); // pass in a JS function
  //new Date(Date.now()); 
  
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
       ,{'$limit': 50}
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
                     'maxFileSize': '1GB',
                     'columnCompression': 'gzip'
                 }
             }
         }
     }
    ];
 
return sellerHist.aggregate(pipeline,{ allowDiskUse: true });
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