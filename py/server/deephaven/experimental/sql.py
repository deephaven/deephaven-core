#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import jpy
from typing import Dict, Optional

from deephaven import DHError
from deephaven.table import Table
from deephaven import jcompat


_JSql = jpy.get_type("io.deephaven.engine.sql.Sql")


def execute_sql(sql: str, scope: Dict[str, Table] = None) -> Table:
    """Experimental SQL execution. Subject to change.

    Args:
        sql (str): the sql
        scope (Dict[str, Table], optional): the scope

    Returns:
        a new Table

    Raises:
        DHError
    """
    try:
        j_table = (
            _JSql.executeSql(sql, jcompat.j_hashmap(scope))
            if scope is not None
            else _JSql.executeSql(sql)
        )
        return Table(j_table=j_table)
    except Exception as e:
        raise DHError(e, "failed to execute SQL.") from e
