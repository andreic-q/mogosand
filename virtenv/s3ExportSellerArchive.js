exports = function () {

   const datalake = context.services.get("FedDB1");
   const db = datalake.db("biquit")
   const events = db.collection("sellerHistArchive");

   const pipeline = [
      { '$limit': 20}
      ,{
        $match: {
           "datetime": {
            //   $gt: new Date(Date.now() - 60 * 60 * 1000),
            $gt: new Date().toISOString().split('T')[0]
              $lt: new Date(Date.now())
           }
        }
      }
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

    /*
      A Scheduled Trigger will always call a function without arguments.
      Documentation on Triggers: https://docs.mongodb.com/realm/triggers/overview/
  
      Functions run by Triggers are run as System users and have full access to Services, Functions, and MongoDB Data.
  
      Access a mongodb service:
      const collection = context.services.get(<SERVICE_NAME>).db("db_name").collection("coll_name");
      const doc = collection.findOne({ name: "mongodb" });
  
      Note: In Atlas Triggers, the service name is defaulted to the cluster name.
  
      Call other named functions if they are defined in your application:
      const result = context.functions.execute("function_name", arg1, arg2);
  
      Access the default http client and execute a GET request:
      const response = context.http.get({ url: <URL> })
  
      Learn more about http client here: https://docs.mongodb.com/realm/functions/context/#context-http
    */
