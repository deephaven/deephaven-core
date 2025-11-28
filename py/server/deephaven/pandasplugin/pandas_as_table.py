#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#

from pandas import DataFrame

from deephaven.pandas import to_table
from deephaven.plugin.object_type import Exporter, FetchOnlyObjectType

NAME = "pandas.DataFrame"


class PandasDataFrameSerializer(FetchOnlyObjectType):
    @property
    def name(self) -> str:
        return NAME

    def is_type(self, object) -> bool:
        return isinstance(object, DataFrame)

    def to_bytes(self, exporter: Exporter, data_frame: DataFrame):
        exporter.reference(to_table(data_frame))
        return b""
