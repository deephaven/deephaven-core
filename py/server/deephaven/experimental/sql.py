#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
import contextlib
import inspect
import jpy
from typing import Dict, Union, Mapping, Optional, Any

from deephaven import DHError
from deephaven.table import Table, _j_py_script_session

_JSql = jpy.get_type("io.deephaven.engine.sql.Sql")


@contextlib.contextmanager
def _scope(globals: Mapping[str, Any], locals: Mapping[str, Any]):
    j_py_script_session = _j_py_script_session()
    # Can do `globals | locals` in Python 3.9+
    j_py_script_session.pushScope({**globals, **locals})
    try:
        yield
    finally:
        j_py_script_session.popScope()


def evaluate(
    sql: str,
    dry_run: bool = False,
) -> Union[Table, jpy.JType]:
    """Evaluates an SQL query and returns the result.

    The query scope is taken from the environment when this method is called.

    Args:
        sql (str): SQL query string
        dry_run (bool, optional): if the query should be a dry run, default is False

    Returns:
        a new Table, or a java TableSpec if dry_run is True

    Raises:
        DHError
    """
    try:
        callers_frame_info = inspect.stack()[1]
        globals = callers_frame_info.frame.f_globals
        locals = callers_frame_info.frame.f_locals
        with _scope(globals, locals):
            if dry_run:
                # No JObjectWrapper for TableSpec
                return _JSql.dryRun(sql)
            else:
                return Table(j_table=_JSql.evaluate(sql))
    except Exception as e:
        raise DHError(e, "failed to execute SQL.") from e
