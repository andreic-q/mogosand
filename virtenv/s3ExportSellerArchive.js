exports = function () {

   const datalake = context.services.get("feddb1-78mhi");
   const db = datalake.db("biquit")
   const events = db.collection("sellerHistArchive");

   const pipeline = [
      { '$limit': 20}
      , {
        '$out': {
            's3': {
                'bucket': 'mongo-atlas-export-test', 
                'region': 'eu-west-2', 
                'filename': 'triggered_sellerHistArchive_limit20', 
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