#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from deephaven.plugin import Registration, Callback
from . import pandas_as_table


class PandasPluginRegistration(Registration):
    @classmethod
    def register_into(clscls, callback: Callback) -> None:
        callback.register(pandas_as_table.PandasDataFrameSerializer)
