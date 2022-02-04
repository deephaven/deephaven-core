from deephaven.plugin import Registration
from . import pandas_as_table

class PandasPluginRegistration(Registration):
    @classmethod
    def register_into(clscls, callback: Registration.Callback) -> None:
        callback.register(pandas_as_table.PandasDataFrameSerializer)
