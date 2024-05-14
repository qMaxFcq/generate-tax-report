from pandas import DataFrame, to_datetime, Timedelta
from datetime import datetime


def get_range_time(
    start_date: datetime, target_end_date: datetime, step_date: float = 30
) -> DataFrame:
    date_ranges = []
    while start_date <= target_end_date:
        end_date = start_date + Timedelta(days=step_date)
        end_date = min(
            end_date, target_end_date
        )  # Adjust end date if it exceeds target end date
        start_date = start_date.replace(
            hour=0, minute=0, second=0
        )  # Set time to 00:00:00
        end_date = end_date.replace(
            hour=23, minute=59, second=59
        )  # Set time to 23:59:59
        start_timestamp = int(start_date.timestamp() * 1000)
        end_timestamp = int(end_date.timestamp() * 1000)
        date_ranges.append((start_timestamp, end_timestamp))
        start_date = end_date + Timedelta(days=1)

        # print(date_ranges)
    return DataFrame(date_ranges, columns=["start_timestamp", "end_timestamp"])
