from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def get_dataframe_from_file(spark: SparkSession, schema: StructType, file_path: str) -> DataFrame:
    """This function reads a space or tab delimited file from the input file path to return a dataframe.
    The input schema and input file path are provided as inputs."""
    # TODO kepp delimiter configurable as space or \t based on the file delimiter specified
    return spark.read.option("delimiter", " "). \
        schema(schema). \
        csv(file_path)


def display_dataframe(data: DataFrame):
    """This function takes a dataframe and outputs its Row objects in a tab separated format"""
    no_cols = len(data.columns)
    for row in data.collect():
        for i in range(0, no_cols):
            print(row[i], end='\t')
        print('\n')
