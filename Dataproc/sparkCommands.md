gcloud dataproc jobs submit spark \
  --cluster=my-cluster \
  --region=us-central1 \
  --class=org.apache.spark.examples.SparkPi \
  --jars=gs://my-bucket/spark-examples.jar \
  --   (delimiter that signals end of gcloud options and beginning of positional job arguments)
  my-app-args

# Sample 1:

gcloud dataproc jobs submit spark \
--cluster=cluster_name \
--region=us-central1 \
--project=project_name \
--class=com.org.example.spark \
--jars=gs://example/example1.jar,gs://example2/example2.jar \
--files=gs://my-bucket/log4j.properties (this will be placed in the working dir of each executor. these files will be copied to each executor before the start of the job and will be deleted once the job completes. this can be useful for jobs that require certain files to be present on each node like configuration files or data files)
--properties=spark.submit.deployMode=cluster,spark.master=yarn,spark.yarn.maxAppAttempts=1,spark.app.name=example,spark.dynamicAllocation.enabled=false,spark.executor.instances=200,spark.executor.cores=3,spark.executor.memory=45G,spark.driver.memory=45G,spark.yarn.executor.memoryOverhead=3G,spark.driver.memoryOverhead=3G,spark.sql.shuffle.partitions=6000,spark.sql.autoBroadcastJoinThreshold=-1 
-- 
load_type="incr" runmode="global" env="prod" start_date="20240412" end_date=${endts}

# Sample 2:

gcloud dataproc jobs submit spark \
--cluster=cluster_name \
--region=us-central1 \
--project=project_name \
--class=com.org.example.spark \
--jars=gs://example/example1.jar,gs://example2/example2.jar \
--files=gs://my-bucket/log4j.properties (this will be placed in the working dir of each executor. these files will be copied to each executor before the start of the job and will be deleted once the job completes. this can be useful for jobs that require certain files to be present on each node like configuration files or data files)
--properties=spark.executor.instances=200,spark.executor.cores=3,spark.executor.memory=45G,spark.driver.memory=45G
-- 
prod gs://sample_bucket/config/file ${path}

# Sample 3: PySpark

gcloud dataproc jobs submit pyspark \
--cluster=cluster_name \
--region=us-central1 \
--project=project_name \
--jars=gs://example/example1.jar,gs://example2/example2.jar \
--properties=spark.executor.instances=200,spark.executor.cores=3,spark.executor.memory=45G,spark.driver.memory=45G
gs://my-bucket/my-pyspark-job.py
-- 
prod gs://sample_bucket/config/file ${path}

# All Propertites:

spark.submit.deployMode=cluster
spark.master=yarn
spark.yarn.maxAppAttempts=1
spark.app.name=example
spark.yarn.executor.memoryOverhead=3G

spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=50
spark.dynamicAllocation.maxExecutors=600
spark.dynamicAllocation.cachedExecutorIdleTimeout=600s

spark.executor.instances=200
spark.executor.cores=3
spark.executor.memory=45G
spark.executor.memoryOverhead=6G
spark.executor.heartbeatInterval=1000000


spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=4G
spark.driver.memory=45G
spark.driver.memoryOverhead=3G
spark.driver.maxResultSize=0
spark.locality.wait=0
spark.memory.fraction=0.75
spark.memory.storageFraction=0.3


spark.sql.shuffle.partitions=6000
spark.sql.autoBroadcastJoinThreshold=-1 
spark.sql.files.maxPartitionBytes=10000000b
spark.sql.sources.partitionOverwriteMode=dynamic
spark.sql.crossJoin.enabled=true
spark.sql.orc.filterPushdown=true
spark.sql.parquet.writeLegacyFormat=true
spark.sql.legacy.charVarcharAsString=true
spark.sql.codegen.wholeStage=false

spark.hadoop.orc.overwrite.output.file=true
spark.hadoop.spark.sql.legacy.parquet.nanosAsLong=false
spark.hadoop.spark.sql.parquet.nanosAsLong=false
spark.hadoop.spark.sql.parquet.binaryAsString=false
spark.hadoop.spark.sql.parquet.int96AsTimestamp=true
spark.hadoop.spark.sql.caseSensitive=false
spark.shuffle.service.enabled=true
spark.serializer=org.apache.spark.serializer.kryoSerializer
spark.network.timeout=1000000
spark.jars.packages=org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.0
spark.scheduler.listenerbus.eventqueue.capacity=90000

# spark.jars.packages

(The parameter spark.jars.packages is used to specify Maven coordinates for external libraries (JAR files) that you want to automatically download and include in your Spark job execution. without having to manually upload JAR files to Google Cloud Storage or specify their locations.

This is particularly useful when you need to include libraries that are not available locally or do not want to manually manage dependencies.

Maven Coordinates: You provide the Maven coordinates in the form of groupId:artifactId:version for each package you want to use.

Automatic Download: Spark will automatically download the required JARs from Maven repositories (e.g., Maven Central) when the job is submitted, making the libraries available on the classpath during execution.)





