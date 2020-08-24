import nltk
from pyspark.sql import functions as F
from pyspark.sql.types import *


def main(spark_dataframe) -> 'spark_dataframe':
    return spark_dataframe