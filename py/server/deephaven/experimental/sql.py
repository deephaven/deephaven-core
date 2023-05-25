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


def eval(
    sql: str,
    dry_run: bool = False,
    globals: Optional[Mapping[str, Any]] = None,
    locals: Optional[Mapping[str, Any]] = None,
) -> Union[Table, jpy.JType]:
    """Experimental SQL evaluation. Subject to change.

    If both globals and locals is omitted (the default), the sql is executed with the globals and locals in the
    environment where eval() is called.

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
        with _scope(globals, locals):
            if dry_run:
                # No JObjectWrapper for TableSpec
                return _JSql.dryRun(sql)
            else:
                return Table(j_table=_JSql.evaluate(sql))
    except Exception as e:
        raise DHError(e, "failed to execute SQL.") from e
