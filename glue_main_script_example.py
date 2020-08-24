import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# install nltk
import os
import site
from importlib import reload
from setuptools.command import easy_install
install_path = site.getsitepackages()[0]
easy_install.main( ["--install-dir", "/home/spark/.local/lib/python3.7/site-packages", "https://files.pythonhosted.org/packages/92/75/ce35194d8e3022203cca0d2f896dbb88689f9b3fce8e9f9cff942913519d/nltk-3.5.zip"] )
reload(site)

import nltk
from pyspark.sql import functions as F
from pyspark.sql.types import *
import bla as alb

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "sample_yelp_reviews", table_name = "test_read", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "sample_yelp_reviews", table_name = "test_read", transformation_ctx = "datasource0")

df = datasource0.toDF()

df = alb.main(df)

#Convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(df, glueContext, "dynamic_frame_write")

datasink2 = glueContext.write_dynamic_frame.from_options(frame = dynamic_frame_write, connection_type = "s3", connection_options = {"path": "s3://glue-mark-test-scripts"}, format = "json", transformation_ctx = "datasink2")
job.commit()