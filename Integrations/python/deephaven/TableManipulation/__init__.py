#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

"""
Functionality to display and modify tables.
"""

import jpy

__all__ = ['ColumnRenderersBuilder', 'DistinctFormatter', 'DownsampledWhereFilter', 'DynamicTableWriter', 
           'LayoutHintBuilder', 'SmartKey', 'SortPair', 'TotalsTableBuilder', 'WindowCheck']

# None until the first successful _defineSymbols() call
ColumnRenderersBuilder = None   #: Class to build and parse the directive for Table.COLUMN_RENDERERS_ATTRIBUTE (io.deephaven.db.v2.ColumnRenderersBuilder).
DistinctFormatter = None        #: Class to create distinct and unique coloration for each unique input value (io.deephaven.db.util.DBColorUtil$DistinctFormatter).
DownsampledWhereFilter = None   #: Class to downsample time series data by calculating the bin intervals for values, and then using upperBin and lastBy to select the last row for each bin (io.deephaven.db.v2.select.DownsampledWhereFilter).
DynamicTableWriter = None       #: Class to create a TableWriter object {@link io.deephaven.db.v2.utils.DynamicTableWriter}
LayoutHintBuilder = None        #: Builder class for use in assembling layout hints suitable for use with {@link io.deephaven.db.tables.Table#layoutHints(LayoutHintBuilder)} or {@link io.deephaven.db.tables.Table#layoutHints(String)} (io.deephaven.db.tables.utils.LayoutHintBuilder).
SmartKey = None                 #: A datastructure key class, where more than one value can be used as the key (io.deephaven.datastructures.util.SmartKey).
TotalsTableBuilder = None       #: Class to define the default aggregations and display for a totals table (io.deephaven.db.v2.TotalsTableBuilder).
SortPair = None                 #: Class representing a column to sort by and its direction (io.deephaven.db.tables.SortPair).


def _defineSymbols():
    """
    Defines appropriate java symbols, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global ColumnRenderersBuilder, DistinctFormatter, DownsampledWhereFilter, DynamicTableWriter, \
        LayoutHintBuilder, SmartKey, TotalsTableBuilder, SortPair

    if ColumnRenderersBuilder is None:
        # This will raise an exception if the desired object is not the classpath
        ColumnRenderersBuilder = jpy.get_type('io.deephaven.db.v2.ColumnRenderersBuilder')
        DistinctFormatter = jpy.get_type('io.deephaven.db.util.DBColorUtil$DistinctFormatter')
        DynamicTableWriter = jpy.get_type('io.deephaven.db.v2.utils.DynamicTableWriter')
        DownsampledWhereFilter = jpy.get_type('io.deephaven.db.v2.select.DownsampledWhereFilter')
        LayoutHintBuilder = jpy.get_type('io.deephaven.db.tables.utils.LayoutHintBuilder')
        SmartKey = jpy.get_type('io.deephaven.datastructures.util.SmartKey')
        TotalsTableBuilder = jpy.get_type('io.deephaven.db.v2.TotalsTableBuilder')
        SortPair = jpy.get_type("io.deephaven.db.tables.SortPair")


# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass
