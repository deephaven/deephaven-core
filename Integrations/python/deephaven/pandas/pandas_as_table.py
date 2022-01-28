from deephaven.python_to_java import dataFrameToTable
from deephaven.plugin.object import Exporter, ObjectType
from pandas import DataFrame

NAME = "pandas.DataFrame"
class PandasDataFrameSerializer(ObjectType):
    @property
    def name(self) -> str:
        return NAME

    def is_type(self, object) -> bool:
        return isinstance(object, DataFrame)

    def to_bytes(self, exporter: Exporter, data_frame: DataFrame):
        exporter.reference(dataFrameToTable(data_frame))
        return b''
