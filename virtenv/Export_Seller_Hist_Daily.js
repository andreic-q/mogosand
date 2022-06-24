exports = function () {

    const coll = "sellerHistArchive"
    const datalake = context.services.get("FedDB1");
    const db = datalake.db("biquit")
    const events = db.collection(coll);
 
    const start = new Date();
    start.setHours(0, 0, 0, 0);
    // console.log(start);
     
    const end = new Date();
    end.setHours(23, 59, 59, 999);
    const start_dt_string = '2022-01-01';
   
   start_date =  new Date(start_dt_string);
   end_date = new Date('2022-01-02'); //new Date(Date.now()); // add one day to it
 
   var splittedDate = splitDate(start_dt_string);
   var year = splittedDate[0];
   var month = splittedDate[1];
   var day = splittedDate[2];
 
    const pipeline = [
       {
         '$match': 
             {datetime: 
                     {
                     '$gte': start_date,
                     '$lt':  end_date
                     }
             }
       }
       ,{'$limit':10}
       , {
         '$out': {
             's3': {
                 'bucket': 'mongo-atlas-export-test', 
                 'region': 'eu-west-2', 
                 'filename': {'$concat':[ coll,"/"
                                         ,year,"/",month,"/",day,"/"]
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
 
   return events.aggregate(pipeline);
 };
 
 //  Sat Dec 31 2022
 // console.log(getPreviousDay(new Date('2023-01-01')));

 function getPreviousDay(date = new Date()) {
   const previous = new Date(date.getTime());
   previous.setDate(date.getDate() - 1);
   return previous;
 }
 
 //split date in format 'YYYY-MM-DD'
 function splitDate(date){
   var result = date.split('-');
   return result;
   
 }