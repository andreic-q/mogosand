This repo contains the code(infrastructure including) for offloading SellerHistory Archive from MongoDB to Datalake

The archive method is parquet files partitioned by day in S3 ( one day of SellerArchive -> one partition in the datalake with prefix YYYY/MM/DD/YYYYMMDD.parquet) files.

offloading script archive_parquet.py
data quality tests: data_tests_atlas_vs_scienceRdFebDB
