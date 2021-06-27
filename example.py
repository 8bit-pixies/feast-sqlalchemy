# This is an example feature definition file

from google.protobuf.duration_pb2 import Duration

from feast import Entity, Feature, FeatureView, ValueType
from feast.data_source import FileSource
from typing import Callable, Dict, Iterable, Optional, Tuple

from pyarrow.parquet import ParquetFile

from feast import type_map
from feast.data_format import FileFormat, StreamFormat


class DataBaseSource(FileSource):
    def __init__(self, 
        event_timestamp_column: Optional[str] = "",
        name: Optional[str] = None,
        path: Optional[str] = None,
        file_format: FileFormat = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
    ):
        self.name = name
        super().__init__(event_timestamp_column, None, path, file_format, created_timestamp_column,field_mapping, date_partition_column)
    

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
driver_hourly_stats = DataBaseSource(
    path="feast_store/data.db",
    name="driver_hourly_stats",
    event_timestamp_column="datetime",
    created_timestamp_column="created",
)

# Define an entity for the driver. You can think of entity as a primary key used to
# fetch features.
driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id",)

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=Duration(seconds=86400 * 1),
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    online=True,
    input=driver_hourly_stats,
    tags={},
)
