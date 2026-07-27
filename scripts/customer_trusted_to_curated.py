import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1785194723962 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1785194723962")

# Script generated for node Customer Trusted
CustomerTrusted_node1785194637898 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1785194637898")

# Script generated for node Join With Accelerometer
SqlQuery1574 = '''
SELECT DISTINCT c.*
FROM customer c
JOIN accelerometer a ON c.email = a.`user`

'''
JoinWithAccelerometer_node1785194849051 = sparkSqlQuery(glueContext, query = SqlQuery1574, mapping = {"customer":CustomerTrusted_node1785194637898, "accelerometer":AccelerometerTrusted_node1785194723962}, transformation_ctx = "JoinWithAccelerometer_node1785194849051")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=JoinWithAccelerometer_node1785194849051, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1785194484713", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1785194982071 = glueContext.getSink(path="s3://stedi-lakehouse-2026/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1785194982071")
customer_curated_node1785194982071.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customer_curated_node1785194982071.setFormat("glueparquet", compression="snappy")
customer_curated_node1785194982071.writeFrame(JoinWithAccelerometer_node1785194849051)
job.commit()