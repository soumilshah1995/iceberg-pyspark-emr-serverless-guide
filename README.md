# iceberg-pyspark-emr-serverless-guide
iceberg-pyspark-emr-serverless-guide



# Steps 

### Step 1: Create EMR Serverless(emr-7.1.0) Cluster

![Screenshot 2024-09-04 at 5 23 52 PM](https://github.com/user-attachments/assets/ee987571-cdd3-4a3d-9332-f198221d8b2a)


### Step 2: Copy Application ID 
```
aws emr-serverless list-applications
```

### Step 3: Create job_iceberg.py 
```
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .config("spark.sql.catalog.dev", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.dev.warehouse", "s3://XX/datalakes/iceberg-dataset/") \
    .config("spark.sql.catalog.dev.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.dev.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .enableHiveSupport() \
    .getOrCreate()


# Initialize the bucket
table_name = "people"
base_path = f"s3://XXX/datalakes/iceberg-dataset/{table_name}"

# Define the records
records = [
    (1, 'John', 25, 'NYC', '2023-09-28 00:00:00'),
    (2, 'Emily', 30, 'SFO', '2023-09-28 00:00:00'),
    (3, 'Michael', 35, 'ORD', '2023-09-28 00:00:00'),
    (4, 'Andrew', 40, 'NYC', '2023-10-28 00:00:00'),
    (5, 'Bob', 28, 'SEA', '2023-09-23 00:00:00'),
    (6, 'Charlie', 31, 'DFW', '2023-08-29 00:00:00')
]

# Define the schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("create_ts", StringType(), True)
])

# Create a DataFrame
df = spark.createDataFrame(records, schema)
df.show()


(
    df.write
    .format("iceberg")
    .mode("overwrite")
    .option("path", base_path)
    .saveAsTable("dev.default.people_iceberg")
)
```



### Upload job on S3  and update ENV VAR

```
aws s3 cp job_iceberg.py s3://soumilshah-dev-1995/jobs/job_iceberg.py


export APPLICATION_ID="XX"
export BUCKET_HUDI="XX
export IAM_ROLE="arn:aws:iam::XX:role/EMRServerlessS3RuntimeRole"
```


### RUN job

```
aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --name "IceBergJobRun" \
    --execution-role-arn $IAM_ROLE \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://XX/jobs/job_iceberg.py",
            "sparkSubmitParameters": "--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.dev.warehouse=s3://XX/warehouse --conf spark.sql.catalog.dev=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.dev.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.sql.catalog.job_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.sources.partitionOverwriteMode=dynamic --conf spark.sql.iceberg.handle-timestamp-without-timezone=true"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://XXlogs/"
            }
        }
    }'

```
