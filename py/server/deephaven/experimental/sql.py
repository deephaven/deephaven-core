#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import inspect
import jpy
from typing import Dict, Union, Mapping, Optional, Any

from deephaven import DHError
from deephaven import jcompat
from deephaven.table import Table

_JSql = jpy.get_type("io.deephaven.engine.sql.Sql")


def _filter_tables(scope: Mapping[str, Any]) -> Dict[str, Table]:
    return {k: v for (k, v) in scope.items() if isinstance(v, Table)}


def eval(
    sql: str,
    dry_run: bool = False,
    globals: Optional[Mapping[str, Any]] = None,
    locals: Optional[Mapping[str, Any]] = None,
) -> Union[Table, jpy.JType]:
    """Experimental SQL evaluation. Subject to change.

    If both globals and locals is omitted, the sql is executed with the globals and locals in the environment where
    eval() is called.

    Args:
        sql (str): the sql
        dry_run (bool, optional): if it should be a dry run, False by default
        globals (Mapping[str, Any], optional): the globals to use
        locals (Mapping[str, Any], optional): the locals to use

    Returns:
        a new Table, or a java TableSpec if dry_run is True

    Raises:
        DHError
    """
    try:
        if globals is None and locals is None:
            callers_frame_info = inspect.stack()[1]
            globals = callers_frame_info.frame.f_globals
            locals = callers_frame_info.frame.f_locals
        else:
            globals = globals or {}
            locals = locals or {}
        # Can use `globals | locals` instead when on python 3.9+
        scope = _filter_tables({**globals, **locals})
        j_scope = jcompat.j_hashmap(scope)
        if dry_run:
            # No JObjectWrapper for TableSpec
            return _JSql.dryRun(sql, j_scope)
        else:
            return Table(j_table=_JSql.evaluate(sql, j_scope))
    except Exception as e:
        raise DHError(e, "failed to execute SQL.") from e
