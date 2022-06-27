**SellerHistory Archival to S3**

This repo contains the code(infrastructure including) for offloading SellerHistory Archive from MongoDB to Datalake

The archive method is parquet files partitioned by day in S3 parquet files.

eg.: one day of SellerArchive -> one partition in the datalake with prefix YYYY/MM/DD/YYYYMMDD.parquet

**Main code:**

- virtual environment can be built from https://github.com/andreic-q/mogosand/tree/main/virtenv/requirements.txt
- offloading script https://github.com/andreic-q/mogosand/tree/main/virtenv/archive_parquet.py
- data quality tests: https://github.com/andreic-q/mogosand/tree/main/virtenv/data_tests_atlas_vs_scienceRdFebDB.py
