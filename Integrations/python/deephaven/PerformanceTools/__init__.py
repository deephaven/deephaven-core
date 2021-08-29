
"""
Tools for users to analyze the performance of the Deephaven system and Deephaven queries.

Note: Some functions return a dictionary of tables and plots.  All of the values in the dictionary can be added to the local
namespace for easy analysis.  For example::
    locals().update(persistentQueryStatusMonitor())
"""

#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

import jpy
import wrapt

_java_type_MetricsManager = None        # None until the first _defineSymbols() call
_java_type_PerformanceQueries = None    # None until the first _defineSymbols() call
_java_type_PerformanceQueriesPerformanceOverviewQueryBuilder = None    # None until the first _defineSymbols() call
_java_type_PerformanceQueriesPersistentQueryStatusMonitor = None # None until the first _defineSymbols() call
_java_type_DBTimeUtils = None           # None until the first _defineSymbols() call


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _java_type_MetricsManager
    if _java_type_MetricsManager is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_MetricsManager = jpy.get_type("io.deephaven.engine.structures.metrics.MetricsManager")

    global _java_type_PerformanceQueries
    if _java_type_PerformanceQueries is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_PerformanceQueries = jpy.get_type("io.deephaven.engine.util.PerformanceQueries")

    global _java_type_PerformanceQueriesPerformanceOverviewQueryBuilder
    if _java_type_PerformanceQueriesPerformanceOverviewQueryBuilder is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_PerformanceQueriesPerformanceOverviewQueryBuilder = jpy.get_type("io.deephaven.engine.util.PerformanceQueries$PerformanceOverview$QueryBuilder")

    global _java_type_PerformanceQueriesPersistentQueryStatusMonitor
    if _java_type_PerformanceQueriesPersistentQueryStatusMonitor is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_PerformanceQueriesPersistentQueryStatusMonitor = jpy.get_type("io.deephaven.engine.util.PerformanceQueries$PersistentQueryStatusMonitor")

    global _java_type_DBTimeUtils
    if _java_type_DBTimeUtils is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_DBTimeUtils = jpy.get_type("io.deephaven.engine.tables.utils.DBTimeUtils")


# every module method should be decorated with @_passThrough
@wrapt.decorator
def _passThrough(wrapped, instance, args, kwargs):
    """
    For decoration of module methods, to define necessary symbols at runtime

    :param wrapped: the method to be decorated
    :param instance: the object to which the wrapped function was bound when it was called
    :param args: the argument list for `wrapped`
    :param kwargs: the keyword argument dictionary for `wrapped`
    :return: the decorated version of the method
    """

    _defineSymbols()
    return wrapped(*args, **kwargs)


# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass


def _javaMapToDict(m):
    """
    Converts a java map to a python dictionary.

    :param m: java map.
    :return: python dictionary.
    """

    result = {}

    for e in m.entrySet().toArray():
        k = e.getKey()
        v = e.getValue()
        result[k] = v

    return result


##################### Performance #####################


@_passThrough
def performanceInfo(db, workerName=None, date=None,):
    """
    Returns a dictionary of tables summarizing the performance of this query.

    :param db: database.
    :param workerName: worker name.  Defaults to the current worker.
    :param date: date to analyze performance on.  Defaults to the current date.
    :return: dictionary of tables summarizing the performance of this query.
    """

    if workerName is None:
        workerName = db.getWorkerName()

    if date is None:
        date = _java_type_DBTimeUtils.currentDateNy()

    if db is not None:
        queryPerformanceLog = db.i2("DbInternal", "QueryPerformanceLog").where(f"Date=`{date}`", f"WorkerName=`{workerName}`");
        updatePerformanceLog = db.i2("DbInternal", "UpdatePerformanceLog").where(f"Date=`{date}`", f"WorkerName=`{workerName}`").update("Ratio=EntryIntervalUsage*100/IntervalDuration");
        topOffenders=updatePerformanceLog.where("Ratio > 1.00").sortDescending("Ratio").update("EntryDescription=(!isNull(EntryDescription) && EntryDescription.length() > 100) ? EntryDescription.substring(0, 100) : EntryDescription");
        performanceAggregate = updatePerformanceLog.view("IntervalEndTime", "EntryIntervalUsage", "IntervalDuration").by("IntervalEndTime").view("IntervalEndTime", "IntervalDuration=first(IntervalDuration)", "Ratio=100*sum(EntryIntervalUsage)/IntervalDuration")
    else:
        queryPerformanceLog = context.i("DbInternal", "QueryPerformanceLog").where(f"Date=`{date}`", f"WorkerName=`{workerName}`");
        updatePerformanceLog = context.i("DbInternal", "UpdatePerformanceLog").where(f"Date=`{date}`", f"WorkerName=`{workerName}`").update("Ratio=EntryIntervalUsage*100/IntervalDuration");
        topOffenders=updatePerformanceLog.where("Ratio > 1.00").sortDescending("Ratio").update("EntryDescription=(!isNull(EntryDescription) && EntryDescription.length() > 100) ? EntryDescription.substring(0, 100) : EntryDescription");
        performanceAggregate = updatePerformanceLog.view("IntervalEndTime", "EntryIntervalUsage", "IntervalDuration").by("IntervalEndTime").view("IntervalEndTime", "IntervalDuration=first(IntervalDuration)", "Ratio=100*sum(EntryIntervalUsage)/IntervalDuration")

    return {
        "queryPerformanceLog": queryPerformanceLog,
        "updatePerformanceLog": updatePerformanceLog,
        "topOffenders": topOffenders,
        "performanceAggregate": performanceAggregate,
    }


@_passThrough
def performanceOverview(db, workerName=None, date=None, useIntraday=True, serverHost=None, workerHostName=None):
    """
    Returns a dictionary of tables containing detailed performance statistics for a query.

    :param db: database.
    :param workerName: worker name.
    :param date: date to analyze performance on.  Defaults to the current date.
    :param useIntraday: true to use performance data stored in intraday tables and false to use performance data stored in historical tables.
    :param serverHost: server host.
    :param workerHostName: worker host name.
    :return: dictionary of tables containing detailed performance statistics.
    """

    if date is None:
        date = _java_type_DBTimeUtils.currentDateNy()

    queryBuilder = _java_type_PerformanceQueriesPerformanceOverviewQueryBuilder() \
        .workerName(workerName) \
        .date(date) \
        .useIntraday(useIntraday) \
        .workerHostName(workerHostName)

    if not useIntraday and _java_type_PerformanceQueries.PERF_QUERY_BY_INTERNAL_PARTITION:
        if db.getServerHost().equals(serverHost):
            serverHost = db.getServerHost().replaceAll("\\.", "_")

        queryBuilder = querybuilder.internalPartition(serverHost)
    else:
        queryBuilder = queryBuilder.hostname(serverHost)

    tables = db.executeQuery(queryBuilder.build())
    return _javaMapToDict(tables)


@_passThrough
def performanceOverviewByName(db, queryName , queryOwner, date=None, useIntraday=True, workerHostName=None, asOfTime=None):
    """
    Returns a dictionary of tables containing detailed performance statistics for a query.
    The query is identfied by the query name and the query owner.

    :param db: database.
    :param queryName: query name.
    :param queryOwner: query owner.
    :param date: date to analyze performance on.  Defaults to the current date.
    :param useIntraday: true to use performance data stored in intraday tables and false to use performance data stored in historical tables.
    :param workerHostName: worker host name.
    :param asOfTime: analyze performance statistics at or before this time.
    :return: dictionary of tables containing performance statistics.
    """

    if date is None:
        date = _java_type_DBTimeUtils.currentDateNy()

    tables = db.executeQuery(
        _java_type_PerformanceQueriesPerformanceOverviewQueryBuilder() \
            .date(date) \
            .queryOwner(queryOwner) \
            .queryName(queryName) \
            .asOfTime(asOfTime) \
            .useIntraday(useIntraday) \
            .workerHostName(workerHostName) \
            .build())

    return _javaMapToDict(tables)


@_passThrough
def persistentQueryStatusMonitor(db, startDate=None, endDate=None):
    """
    Returns a dictionary of tables containing data and performance statistics for all persistent queries.
    This enables you to see if your queries are running and provides various metrics related to their performance.

    :param db: database.
    :param startDate: start of the date range to analyze.  The date format is `YYYY-MM-DD`.  Defaults to the current date.
    :param endDate: end of the date range to analyze.  The date format is `YYYY-MM-DD`.  Defaults to the current date.
    :return: dictionary of tables containing data and performance statistics for all persistent queries.
    """

    if startDate is None and endDate is None:
        tables = db.executeQuery(_java_type_PerformanceQueriesPersistentQueryStatusMonitor())
    elif endDate is None:
        tables = db.executeQuery(_java_type_PerformanceQueriesPersistentQueryStatusMonitor(startDate))
    else:
        tables = db.executeQuery(_java_type_PerformanceQueriesPersistentQueryStatusMonitor(startDate,endDate))

    return _javaMapToDict(tables)


#################### Count Metrics ##########################


@_passThrough
def metricsCountsReset():
    """
    Resets Deephaven performance counter metrics.
    """
    _java_type_MetricsManager.resetCounters()

@_passThrough
def metricsCountsGet():
    """
    Gets Deephaven performance counter metrics.

    :return: Deephaven performance counter metrics.
    """
    return _java_type_MetricsManager.getCounters()

@_passThrough
def metricsCountsPrint():
    """
    Prints Deephaven performance counter metrics.
    """
    print(metricsCountsGet())
