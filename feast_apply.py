from feast import FeatureStore
from feast.repo_config import RepoConfig, load_repo_config
from pathlib import Path
from offlinestore import SQLiteOfflineStore
import duckdb

from example import driver_hourly_stats_view, driver

repo_path = "feast_store"
config = load_repo_config(Path(repo_path))
config.offline_store = SQLiteOfflineStore()

store = FeatureStore(config=config)
store.apply([driver_hourly_stats_view, driver])

entity_df = duckdb.query("select * from 'feast_store/data/driver_stats.parquet' limit 10").to_df()[["datetime", "driver_id"]]
training_df = store.get_historical_features(
    entity_df=entity_df,
    feature_refs=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
).to_df()

print(training_df)