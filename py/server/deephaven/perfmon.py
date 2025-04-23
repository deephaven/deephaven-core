#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#

""" Tools to obtain internal, Deephaven logs as tables, and tools to analyze the performance of the Deephaven
system and Deephaven queries.
"""
from typing import Dict, Union, List

import jpy
import base64

from deephaven import DHError
from deephaven.jcompat import j_map_to_dict
from deephaven.table import Table, TreeTable
from deephaven.update_graph import auto_locking_ctx

_JPerformanceQueries = jpy.get_type("io.deephaven.engine.table.impl.util.PerformanceQueries")
_JMetricsManager = jpy.get_type("io.deephaven.util.metrics.MetricsManager")
_JTableLoggers = jpy.get_type("io.deephaven.engine.table.impl.util.TableLoggers")
_JUpdateAncestorViz = jpy.get_type("io.deephaven.engine.table.impl.util.UpdateAncestorViz")
_JString = jpy.get_type("java.lang.String")
_JFile = jpy.get_type("java.io.File")


def process_info_log() -> Table:
    """ Returns a static table with process information for the current Deephaven engine process.

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableLoggers.processInfoLog())
    except Exception as e:
        raise DHError(e, "failed to obtain the process info log table.") from e


def server_state_log() -> Table:
    """ Returns a table with memory utilization, update graph processor and garbage collection stats
    sampled on a periodic basis.

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableLoggers.serverStateLog())
    except Exception as e:
        raise DHError(e, "failed to obtain the server state log table.") from e


def process_metrics_log() -> Table:
    """ Returns a table with metrics collected for the current Deephaven engine process.

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableLoggers.processMetricsLog())
    except Exception as e:
        raise DHError(e, "failed to obtain the process metrics log table.") from e


def query_operation_performance_log() -> Table:
    """ Returns a table with Deephaven performance data for individual subqueries. Performance data for the entire query
    is available from calling 'query_performance_log'.

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableLoggers.queryOperationPerformanceLog())
    except Exception as e:
        raise DHError(e, "failed to obtain the query operation performance log table.") from e


def query_performance_log() -> Table:
    """ Returns a table with Deephaven query performance data. Performance data for individual sub-operations is
    available from calling `query_operation_performance_log`.

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableLoggers.queryPerformanceLog())
    except Exception as e:
        raise DHError(e, "failed to obtain the query performance log table.") from e


def query_operation_performance_tree_table() -> TreeTable:
    """ Returns a tree table with Deephaven performance data for individual subqueries.

    Returns:
        a TreeTable

    Raises:
        DHError
    """
    try:
        with auto_locking_ctx(query_performance_log()):
            return TreeTable(j_tree_table=_JPerformanceQueries.queryOperationPerformanceAsTreeTable(),
                             id_col="EvalKey", parent_col="ParentEvalKey")
    except Exception as e:
        raise DHError(e, "failed to obtain the query operation performance log as tree table.") from e


def query_performance_tree_table() -> TreeTable:
    """ Returns a tree table with Deephaven query performance data. Performance data for individual sub-operations as
    a tree table is available from calling `query_operation_performance_tree_table`.

    Returns:
        a TreeTable

    Raises:
        DHError
    """
    try:
        with auto_locking_ctx(query_performance_log()):
            return TreeTable(j_tree_table=_JPerformanceQueries.queryPerformanceAsTreeTable(),
                             id_col="EvaluationNumber", parent_col="ParentEvaluationNumber")
    except Exception as e:
        raise DHError(e, "failed to obtain the query performance log as tree table.") from e


def update_performance_log() -> Table:
    """ Returns a table with Deephaven update performance data.

    Returns
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableLoggers.updatePerformanceLog())
    except Exception as e:
        raise DHError(e, "failed to obtain the update performance log table.") from e


def update_performance_ancestors_log() -> Table:
    """ Returns a table with Deephaven update performance ancestor data.

    Returns
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableLoggers.updatePerformanceAncestorsLog())
    except Exception as e:
        raise DHError(e, "failed to obtain the update performance log table.") from e


def metrics_reset_counters() -> None:
    """ Resets Deephaven performance counter metrics. """
    _JMetricsManager.resetCounters()


def metrics_get_counters() -> str:
    """ Gets Deephaven performance counter metrics.

    Returns:
        a string of the Deephaven performance counter metrics.
    """
    return _JMetricsManager.getCounters()


def process_info(proc_id: str, proc_type: str, key: str) -> str:
    """ Gets the information for a process.

    Args:
        proc_id (str): the process id
        proc_type (str): the process type
        key (str): the key of the process property

    Returns:
        a string of process information

    Raises:
        DHError
    """
    try:
        return _JPerformanceQueries.processInfo(proc_id, proc_type, key)
    except Exception as e:
        raise DHError(e, "failed to obtain the process info.") from e


def server_state() -> Table:
    """ Returns a table of basic memory, update graph processor, and GC stats for the current engine process,
    sampled on a periodic basis.

    Returns:
        a table
    """
    try:
        return Table(j_table=_JPerformanceQueries.serverState())
    except Exception as e:
        raise DHError(e, "failed to produce a table with server state info.") from e


def query_operation_performance(eval_number: int) -> Table:
    """ Takes in a query evaluation number and returns a view for that query's individual operation's performance data.

    You can obtain query evaluation numbers, which uniquely identify a query and its subqueries, via the performance
    data tables obtained from calling query_performance_log() or query_operation_performance_log()

    The query operation performance log contains data on how long each individual operation of a query (where(),
    update(), naturalJoin(), etc., as well as internal functions) takes to execute, and the change in resource
    consumption while each was executing.

    Args:
        eval_number (int): the evaluation number

    Returns:
        a table of query operation performance data

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JPerformanceQueries.queryOperationPerformance(eval_number))
    except Exception as e:
        raise DHError(e, "failed to obtain the query operation performance data.") from e


def query_performance(eval_number: int) -> Table:
    """ Takes in a query evaluation number and returns a view for that query's performance data.

    You can obtain query evaluation numbers, which uniquely identify a query and its subqueries, via the performance
    data tables obtained from calling query_performance_log() or query_operation_performance_log()

    The query performance log contains data on how long each query takes to run. Examples of what constitutes one
    individual query, for performance logging purposes, include:

        * A new command in the console (i.e. type something, then press the return key)
        * A sort, filter, or custom column generated by a UI
        * A call from a client API external application

    Args:
        eval_number (int): the evaluation number

    Returns:
        a Table of query performance data

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JPerformanceQueries.queryPerformance(eval_number))
    except Exception as e:
        raise DHError(e, "failed to obtain the query performance data.") from e


def query_update_performance(eval_number: int) -> Table:
    """  Takes in a query evaluation number and returns a view for that query's update performance data.

    You can obtain query evaluation numbers, which uniquely identify a query and its subqueries, via the performance
    data tables obtained from calling query_performance_log() or query_operation_performance_log()

    Args:
        eval_number (int): the evaluation number

    Returns:
        a Table of query update performance data

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JPerformanceQueries.queryUpdatePerformance(eval_number))
    except Exception as e:
        raise DHError(e, "failed to obtain the query update performance data.") from e


def query_update_performance_map(eval_number: int) -> Dict[str, Table]:
    """ Creates multiple tables with performance data for a given query identified by an evaluation number. The tables
    are returned in a map with the following String keys: 'QueryUpdatePerformance', 'UpdateWorst', 'WorstInterval',
    'UpdateMostRecent', 'UpdateAggregate', 'UpdateSummaryStats'.

    Args:
        eval_number (int): the evaluation number

    Returns:
        a dict

    Raises:
        DHError
    """

    try:
        d = j_map_to_dict(_JPerformanceQueries.queryUpdatePerformanceMap(eval_number))
        for k in d.keys():
            d[k] = Table(j_table=d[k])
        return d
    except Exception as e:
        raise DHError(e, "failed to obtain the query update perf map.") from e


def ancestor_svg(ids: Union[List[int], int], update_perf_log: Table, ancestors_log: Table, filename: str = None) -> str:
    """ Returns the contents of an SVG image containing a graph of the ancestor hierarchy derived from the passed in
    UpdatePerformanceLog and UpdatePerformanceAncestorsLog for the provided Performance Entry identifier. This can be used
    to help understand the structure of a query.

    Args:
        ids (Union[List[int], int]): the Performance entry identifier or identifiers (EntryId) to generate the graph for
        update_perf_log (Table): the UpdatePerformanceLog Table
        ancestors_log (Table): the UpdatePerformanceAncestorsLog Table
        filename (str): the name of the output SVG file or None to not write the file, default is None

    Returns
        the contents of an SVG image

    Raises:
        DHError
    """
    try:
        if isinstance(ids, int):
            ids = [ids]
        j_file = _JFile(filename) if filename is not None else None
        svg_bytes = _JUpdateAncestorViz.svg(ids, update_perf_log.j_table, ancestors_log.j_table, j_file)
        return str(_JString(svg_bytes))
    except Exception as e:
        raise DHError(e, "failed to produce ancestor SVG") from e


def ancestor_image(ids: Union[List[int], int], update_perf_log: Table, ancestors_log: Table) -> "deephaven.ui.Element":
    """ Returns a deephaven.ui component with an embedded SVG image containing the hierarchy derived from the passed in
    UpdatePerformanceLog and UpdatePerformanceAncestorsLog for the provided Performance Entry identifier.  This can be used
    to help understand the structure of a query.

    Note that the deephaven-plugin-ui package must be installed to use this function.

    Args:
        ids (Union[List[int], int]): the Performance entry identifier or identifiers (EntryId) to generate the graph for
        update_perf_log (Table): the UpdatePerformanceLog Table
        ancestors_log (Table): the UpdatePerformanceAncestorsLog Table

    Returns
        a UI component with an embedded graph of ancestors

    Raises:
        DHError
    """

    try:
        import deephaven.ui
        image_contents = ancestor_svg(ids, update_perf_log, ancestors_log).encode('utf-8')
        return deephaven.ui.image(f"data:image/svg+xml;base64,{base64.b64encode(image_contents).decode()}")
    except ImportError:
        raise Exception("deephaven.ui is not available, consider \"pip install deephaven-plugin-ui\" in your Python virtual environment")
    except Exception as e:
        raise DHError(e, "failed to produce ancestor image") from e


def ancestor_dot(ids: Union[List[int], int], update_perf_log: Table, ancestors_log: Table) -> str:
    """ Returns a graphviz DOT representing Deephaven update performance ancestor data.

    Args:
        ids (Union[List[int], int]): the Performance entry identifier or identifiers (EntryId) to generate the graph for
        update_perf_log (Table): the UpdatePerformanceLog Table
        ancestors_log (Table): the UpdatePerformanceAncestorsLog Table

    Returns
        a string of graphviz DOT format data

    Raises:
        DHError
    """
    try:
        if isinstance(ids, int):
            ids = [ids]
        return _JUpdateAncestorViz.dot(ids, update_perf_log.j_table, ancestors_log.j_table)
    except Exception as e:
        raise DHError(e, "failed to produce ancestor DOT file") from e
