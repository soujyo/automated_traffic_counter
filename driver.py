from pyspark.sql.types import IntegerType, StructField, StringType

from data_utils import *
from src.vehicle_data_transformation.vehicle_counts import VehicleCountAggregation

SPARK_SESSION: SparkSession = (SparkSession.builder
                               .master('local[*]')
                               .appName('PySpark-driver-app')
                               .config("spark.sql.shuffle.partitions", "20")
                               .config('spark.port.maxRetries', 10)
                               .getOrCreate())
INPUT_FILE_PATH: str = 'input_data.csv' #TODO make path to be picked from config file

if __name__ == "__main__":
    # Define the input data schema
    input_schema = StructType([
        StructField('event_timestamp', StringType()),
        StructField('counts', IntegerType())
    ])

    # Read data from the file
    input_df = get_dataframe_from_file(SPARK_SESSION, input_schema, INPUT_FILE_PATH)
    input_df.cache()
    vehicle_data_obj = VehicleCountAggregation(input_df)
    # The number of cars seen in total
    total_no_cars = vehicle_data_obj.get_total_vehicle_counts().collect()[0][0]
    print(f"The number of cars seen in total: {total_no_cars}")
    # The total number of cars per day
    output_df = vehicle_data_obj.get_total_vehicle_counts_per_day()
    print('The total number of cars seen per day')
    display_dataframe(output_df)
    # The top 3 half hours having the most cars
    output_df = vehicle_data_obj.get_top_n_half_hours_with_max_vehicle_count(3)
    print('The top 3 half hours having the most cars')
    display_dataframe(output_df)

    # The period of 3 contiguous half hour(1.5hours) records with the least cars
    output_df = vehicle_data_obj.get_period_of_n_half_hours_with_least_cars(3)
    print('The period of 3 contiguous half hour(1.5hours) records with the least cars')
    display_dataframe(output_df)
