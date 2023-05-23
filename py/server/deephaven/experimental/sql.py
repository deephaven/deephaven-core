#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import jpy
from typing import Dict, Union

from deephaven import DHError
from deephaven.table import Table
from deephaven import jcompat


_JSql = jpy.get_type("io.deephaven.engine.sql.Sql")


def execute(sql: str, scope: Dict[str, Table] = None, dry_run: bool = False) -> Union[Table, jpy.JType]:
    """Experimental SQL execution. Subject to change.

    Args:
        sql (str): the sql
        scope (Dict[str, Table], optional): the scope
        dry_run (bool, optional): if it should be a dry run, False by default

    Returns:
        a new Table

    Raises:
        DHError
    """
    try:
        if dry_run:
            j_table_spec = (
                _JSql.dryRun(sql, jcompat.j_hashmap(scope))
                if scope is not None
                else _JSql.dryRun(sql)
            )
            # No JObjectWrapper for TableSpec
            return j_table_spec
        else:
            j_table = (
                _JSql.execute(sql, jcompat.j_hashmap(scope))
                if scope is not None
                else _JSql.execute(sql)
            )
            return Table(j_table=j_table)
    except Exception as e:
        raise DHError(e, "failed to execute SQL.") from e
