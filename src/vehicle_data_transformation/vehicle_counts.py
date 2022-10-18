from pyspark.sql import DataFrame
from pyspark.sql.functions import date_format, rank, col, lag, sum as _sum
from pyspark.sql.window import Window


class VehicleCountAggregation:
    """Perform vehicle count aggregations"""
    DATE_FORMAT = 'yyyy-MM-dd'

    def __init__(self, vehicle_data: DataFrame):
        self.__vehicle_data = vehicle_data

    def get_total_vehicle_counts(self) -> DataFrame:
        """Get the overall total vehicle count so far"""
        return self.__vehicle_data.select("counts"). \
            agg({"counts": 'sum'}).withColumnRenamed("SUM(counts)", "total_vehicle_count")

    def get_total_vehicle_counts_per_day(self) -> DataFrame:
        """Get the total vehicle counts per day"""
        return self.__vehicle_data.select(
            date_format("event_timestamp", VehicleCountAggregation.DATE_FORMAT).alias('date'), "counts").groupBy(
            'date').agg(
            {"counts": 'sum'}).withColumnRenamed("SUM(counts)",
                                                 "total_vehicle_count"). \
            drop("event_timestamp"). \
            orderBy("date")

    def get_top_n_half_hours_with_max_vehicle_count(self, top_n: int) -> DataFrame:
        """Get the top n half hours having the most vehicles"""
        window_spec = Window.orderBy(
            self.__vehicle_data['counts'].desc())
        return self.__vehicle_data.select('*', rank().over(window_spec).alias('rank')).filter(
            col('rank') <= top_n).drop("rank")

    def get_period_of_n_half_hours_with_least_cars(self, n_half_hours: int) -> DataFrame:
        """Get the period of n contiguous half hour records with the least vehicles count"""
        window_spec_lag = Window.orderBy(self.__vehicle_data['event_timestamp'])
        window_spec_sum = Window.orderBy(self.__vehicle_data['event_timestamp']).rowsBetween(0 - (n_half_hours - 1), 0)
        # .rowsBetween(0 - n_half_hours, 0)
        vehicle_data_agg = self.__vehicle_data.select('*', lag('event_timestamp', n_half_hours - 1).over(
            window_spec_lag).alias('prev_event_timestamp'),
                                                      _sum('counts').over(window_spec_sum).alias(
                                                          f'rolling_sum_{n_half_hours}_half_hours')) \
            .filter(col('prev_event_timestamp').isNotNull())
        vehicle_data_agg.cache()
        least_vehicles_for_n_half_hours = vehicle_data_agg.agg(
            {f'rolling_sum_{n_half_hours}_half_hours': "min"}).collect()[0][0]
        vehicle_data_least_count = vehicle_data_agg.filter(
            col(f'rolling_sum_{n_half_hours}_half_hours') == least_vehicles_for_n_half_hours) \
            .select('event_timestamp')
        return vehicle_data_least_count
