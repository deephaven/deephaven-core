#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

"""
Functionality to display and modify tables.
"""

import jpy

__all__ = ['Aggregation', 'ColumnRenderersBuilder', 'DistinctFormatter', 'DownsampledWhereFilter', 'DynamicTableWriter',
           'LayoutHintBuilder', 'Replayer', 'SmartKey', 'SortColumn', 'TotalsTableBuilder', 'WindowCheck']

# None until the first successful _defineSymbols() call
Aggregation = None       #: Class to supply helpers for constructing aggregations
ColumnName = None               #: Class to represent a column name
ColumnRenderersBuilder = None   #: Class to build and parse the directive for Table.COLUMN_RENDERERS_ATTRIBUTE (io.deephaven.engine.util.ColumnRenderersBuilder).
DistinctFormatter = None        #: Class to create distinct and unique coloration for each unique input value (io.deephaven.engine.util.ColorUtil$DistinctFormatter).
DownsampledWhereFilter = None   #: Class to downsample time series data by calculating the bin intervals for values, and then using upperBin and lastBy to select the last row for each bin (io.deephaven.engine.table.impl.select.DownsampledWhereFilter).
DynamicTableWriter = None       #: Class to create a TableWriter object {@link io.deephaven.engine.table.impl.util.DynamicTableWriter}
LayoutHintBuilder = None        #: Builder class for use in assembling layout hints suitable for use with {@link io.deephaven.engine.table.Table#setLayoutHints(LayoutHintBuilder)} or {@link io.deephaven.engine.table.Table#setLayoutHints(String)} (io.deephaven.engine.util.LayoutHintBuilder).
Replayer = None                 #: Class to create a Replayer object {@link io.deephaven.engine.table.impl.replay.Replayer}
SmartKey = None                 #: A datastructure key class, where more than one value can be used as the key (io.deephaven.datastructures.util.SmartKey).
SortColumn = None                #: Class representing a column to sort by and its direction (io.deephaven.api.Sortcolumn).
TotalsTableBuilder = None       #: Class to define the default aggregations and display for a totals table (io.deephaven.engine.util.TotalsTableBuilder).

def _defineSymbols():
    """
    Defines appropriate java symbols, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global Aggregation, ColumnName, ColumnRenderersBuilder, DistinctFormatter, DownsampledWhereFilter, DynamicTableWriter, \
        LayoutHintBuilder, Replayer, SmartKey, SortColumn, TotalsTableBuilder

    if Aggregation is None:
        # This will raise an exception if the desired object is not the classpath
        Aggregation = jpy.get_type('io.deephaven.api.agg.Aggregation')
        ColumnName = jpy.get_type("io.deephaven.api.ColumnName")
        ColumnRenderersBuilder = jpy.get_type('io.deephaven.engine.util.ColumnRenderersBuilder')
        DistinctFormatter = jpy.get_type('io.deephaven.engine.util.ColorUtil$DistinctFormatter')
        DownsampledWhereFilter = jpy.get_type('io.deephaven.engine.table.impl.select.DownsampledWhereFilter')
        DynamicTableWriter = jpy.get_type('io.deephaven.engine.table.impl.util.DynamicTableWriter')
        LayoutHintBuilder = jpy.get_type('io.deephaven.engine.util.LayoutHintBuilder')
        Replayer = jpy.get_type('io.deephaven.engine.table.impl.replay.Replayer')
        SmartKey = jpy.get_type('io.deephaven.datastructures.util.SmartKey')
        SortColumn = jpy.get_type('io.deephaven.api.SortColumn')
        TotalsTableBuilder = jpy.get_type('io.deephaven.engine.util.TotalsTableBuilder')


# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass
