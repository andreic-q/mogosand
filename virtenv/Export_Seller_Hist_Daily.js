exports = function () {

  const coll = "sellerHistArchive";
  const datalake = context.services.get("FedDB1");
  const db = datalake.db("biquit");
  const events = db.collection(coll);
     
  const start_dt_string = '2022-01-01';
  // const end_date = new Date('2022-01-02'); //new Date(Date.now());
  const end_date = getNextDay(new Date(start_dt_string));

  var splittedDate = splitDate(start_dt_string);
  const out_path=splittedDate.join('/');
  console.log(out_path);
 
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
                                         ,"/"]
                             }, 
                 'format': {
                     'name': 'parquet', 
                     'maxFileSize': '500MiB',
                     'columnCompression': 'gzip'
                 }
             }
         }
     }
     
    ];
 
   return events.aggregate(pipeline,{ allowDiskUse: true });
 };
 
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
 
 //split date in format 'YYYY-MM-DD'
 function splitDate(date){
   var result = date.split('-');
   return result;
   
 }