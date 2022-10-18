from pyspark.sql.types import StructType, StringType, StructField, IntegerType, Row

from src.tests.PySparkTest import PySparkTest
from src.vehicle_data_transformation.vehicle_counts import VehicleCountAggregation


class TestVehicleCountAggregation(PySparkTest):

    def test_should_get_total_vehicle_counts(self):
        """Test to verify the total vehicles seen for all days"""
        input_schema = StructType([
            StructField('event_timestamp', StringType(), False),
            StructField('counts', IntegerType(), True)
        ])
        input_data = [("2021-12-01T05:00:00", 5),
                      ("2021-12-01T05:30:00", 3),
                      ("2021-12-05T09:30:00", 10),
                      ("2021-12-05T10:30:00", 0),
                      ("2021-12-05T11:30:00", 15),
                      ("2021-12-08T18:00:00", 5),
                      ("2021-12-08T18:30:00", 19)]

        input_dataframe = self.df_of(input_data, input_schema)
        aggregated_dataframe = VehicleCountAggregation(input_dataframe).get_total_vehicle_counts()
        expected_schema = StructType([
            StructField('total_vehicle_count', IntegerType())
        ])

        expected_data = [Row(total_vehicle_count=57)]
        expected_dataframe = self.df_of(expected_data, expected_schema)
        self.assertDataFrameEqual(aggregated_dataframe, expected_dataframe)

    def test_should_get_total_cars_per_day(self):
        input_schema = StructType([
            StructField('event_timestamp', StringType(), False),
            StructField('counts', IntegerType(), True)
        ])
        input_data = [("2021-12-01T05:00:00", 5),
                      ("2021-12-01T05:30:00", 3),
                      ("2021-12-05T09:30:00", 10),
                      ("2021-12-05T10:30:00", 0),
                      ("2021-12-05T11:30:00", 15),
                      ("2021-12-08T18:00:00", 5),
                      ("2021-12-08T18:30:00", 19)]

        input_dataframe = self.df_of(input_data, input_schema)
        aggregated_dataframe = VehicleCountAggregation(input_dataframe).get_total_vehicle_counts_per_day()
        expected_schema = StructType([
            StructField('date', StringType()),
            StructField('total_vehicle_count', IntegerType())
        ])

        expected_data = [('2021-12-01', 8),
                         ('2021-12-05', 25),
                         ('2021-12-08', 24)]

        expected_dataframe = self.df_of(expected_data, expected_schema)

        self.assertDataFrameEqual(aggregated_dataframe, expected_dataframe)

    def test_should_get_top_n_half_hours_with_max_vehicle_count(self):
        top_n = 3
        input_schema = StructType([
            StructField('event_timestamp', StringType(), False),
            StructField('counts', IntegerType(), True)
        ])
        input_data = [("2021-12-01T05:00:00", 5),
                      ("2021-12-01T05:30:00", 3),
                      ("2021-12-01T06:00:00", 11),
                      ("2021-12-01T06:30:00", 14),
                      ("2021-12-01T05:30:00", 3),
                      ("2021-12-05T09:30:00", 10),
                      ("2021-12-05T10:30:00", 0),
                      ("2021-12-05T11:30:00", 15),
                      ("2021-12-08T18:00:00", 5),
                      ("2021-12-08T18:30:00", 19)]

        input_dataframe = self.df_of(input_data, input_schema)
        aggregated_dataframe = VehicleCountAggregation(input_dataframe).get_top_n_half_hours_with_max_vehicle_count(
            top_n)
        expected_schema = StructType([
            StructField('event_timestamp', StringType()),
            StructField('counts', IntegerType())
        ])

        expected_data = [('2021-12-08T18:30:00', 19),
                         ('2021-12-05T11:30:00', 15),
                         ('2021-12-01T06:30:00', 14)]

        expected_dataframe = self.df_of(expected_data, expected_schema)

        self.assertDataFrameEqual(aggregated_dataframe, expected_dataframe)

    def test_should_get_period_of_n_half_hours_with_least_cars(self):
        n_half_hours = 3
        input_schema = StructType([
            StructField('event_timestamp', StringType(), False),
            StructField('counts', IntegerType(), True)
        ])
        input_data = [("2021-12-01T05:00:00", 15),
                      ("2021-12-01T05:30:00", 3),
                      ("2021-12-01T06:00:00", 11),
                      ("2021-12-01T06:30:00", 14),
                      ("2021-12-05T09:30:00", 10),
                      ("2021-12-05T10:30:00", 0),
                      ("2021-12-05T11:30:00", 15),
                      ("2021-12-08T18:00:00", 5),
                      ("2021-12-08T18:30:00", 19)]

        input_dataframe = self.df_of(input_data, input_schema)
        aggregated_dataframe = VehicleCountAggregation(input_dataframe).get_period_of_n_half_hours_with_least_cars(
            n_half_hours)
        expected_schema = StructType([
            StructField('event_timestamp', StringType())
        ])

        expected_data = [Row(event_timestamp='2021-12-08T18:00:00')]

        expected_dataframe = self.df_of(expected_data, expected_schema)

        self.assertDataFrameEqual(aggregated_dataframe, expected_dataframe)

# if __name__ == '__main__':
#     unittest.main()
