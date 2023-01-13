import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


predicate_pushdown = "region in ('ca','gb','us')"


# Script generated for node Amazon S3
AmazonS3_node1669744760171 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://de-youtubedata-raw-useast1-dev/youtube/raw_statistics/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1669744760171",
    push_down_predicate = predicate_pushdown
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1669744782353 = ApplyMapping.apply(
    frame=AmazonS3_node1669744760171,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "bigint", "category_id", "bigint"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "bigint", "views", "bigint"),
        ("likes", "bigint", "likes", "bigint"),
        ("dislikes", "bigint", "dislikes", "bigint"),
        ("comment_count", "bigint", "comment_count", "bigint"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1669744782353",
)

resolvechoice = ResolveChoice.apply(frame = ChangeSchemaApplyMapping_node1669744782353, choice = "make_struct", transformation_ctx = "resolvechoice")
dropnullfields = DropNullFields.apply(frame = resolvechoice, transformation_ctx = "dropnullfields")


datasink1 = dropnullfields.toDF().coalesce(1)
df_final_output = DynamicFrame.fromDF(datasink1, glueContext, "df_final_output")

# Script generated for node Amazon S3
AmazonS3_node1669744784297 = glueContext.write_dynamic_frame.from_options(
    frame=df_final_output,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://de-youtubedata-cleansed-useast1-dev/youtube/raw_statistics/",
        "partitionKeys": ["region"]
    },
    format = "parquet",
    transformation_ctx="AmazonS3_node1669744784297",
)


job.commit()
