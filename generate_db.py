import sys

sys.path.append("feast_store")

from example import driver_hourly_stats_view, driver_hourly_stats
from sqlalchemy import create_engine
import pandas as pd

df = pd.read_parquet("feast_store/data/driver_stats.parquet")
engine = create_engine("sqlite:///feast_store/{}".format(driver_hourly_stats.path))
df.created = pd.to_datetime(df.created)
df.datetime = pd.to_datetime(df.datetime)
df.to_sql(driver_hourly_stats_view.name, con=engine, if_exists='replace')

