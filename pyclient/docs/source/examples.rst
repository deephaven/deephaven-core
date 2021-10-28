
Python Client Examples
#####################

This page shows how to perform common operations with the Deephaven Python Client.

Initialize
##########

The `Session` class is your connection to Deephaven. This is where all operations start:

    from pydeephaven import Session

    #session = Session(host=”envoy”) # Use this if you’re running the python script in docker-compose with the Deephaven default settings
    session = Session() # Use this if you’re running the python script locally with with Deephaven default settings

Ticking table
#############

The `Session` class has many methods that create tables. This example creates a ticking time table and binds it to Deephaven:

    from pydeephaven import Session

    session = Session()

    table = session.time_table(period=1000000).update(formulas=["Col1 = i % 2"])
    session.bind_table(name="MyTable", table=table)

This is the general flow of how the Python client interacts with Deephaven. You create a table (new or existing), execute some operations on it, and then bind it to Deephaven.

If you don’t bind the table, it will not be updated in Deephaven.

Execute a query on a table
##########################

`table.update()` can be used to execute an update on a table. This example updates a table with a query string:

    from pydeephaven import Session

    session = Session()

    table = session.empty_table(3)
    table = table.update(["MyColumn = i"])
    session.bind_table(name="MyTable", table=table)

Query objects
#############

Query objects can be used to execute queries on a table. The value of a query object comes from the fact that a connection to Deephaven is only made once for all queries, whereas executing queries directly on a table causes a connection to be made for every query.

The general flow of using a query object is to construct a query with a table, call operations (sort, filter, update, etc.) on the query object, and then assign your table to `query.exec()`.

Any operation that can be executed on a table can also be executed on a query object. This example uses a query object to produce the same result as above:

    from pydeephaven import Session

    session = Session()

    table = session.empty_table(3)

    query = session.query(table)
    query = query.update(["MyColumn = i"])

    table = query.exec()
    session.bind_table(name="MyTable", table=table)

Sort a table
############

`table.sort()` can be used to sort a table. This example sorts a table by one of its columns:

    from pydeephaven import Session

    session = Session()

    table = session.empty_table(5)
    table = table.update(["SortColumn = 4-i"])

    table = table.sort(["SortColumn"])
    session.bind_table(name="MyTable", table=table)

Filter a table
##############

`table.where()` can be used to filter a table. This example filters a table using a filter string:

    from pydeephaven import Session

    session = Session()

    table = session.empty_table(5)
    table = table.update(["Values = i"])

    table = table.where(["Values % 2 == 1"])
    session.bind_table(name="MyTable", table=table)

Join 2 tables
#############

`table.join()` is one of many operations that can join two tables, as shown below:

    from pydeephaven import Session

    session = Session()

    table1 = session.empty_table(5)
    table1 = table1.update(["Values1 = i", "Group = i"])
    table2 = session.empty_table(5)
    table2 = table2.update(["Values2 = i + 10", "Group = i"])

    table = table1.join(table2, on=["Group"])
    session.bind_table(name="MyTable", table=table)

Use a combo aggregation on a table
##################################

Combined aggregations can be executed on tables in the Python client. This example creates a combo aggregation that averages the `Count` column of a table, and aggregates it by the `Group` column::

    from pydeephaven import Session, ComboAggregation

    session = Session()

    table = session.empty_table(10)
    table = table.update(["Count = i", "Group = i % 2"])

    my_agg = ComboAggregation()
    my_agg = my_agg.avg(["Count"])

    table = table.combo_by(["Group"], my_agg)
    session.bind_table(name="MyTable", table=table)

Convert a pyarrow table to a Deephaven table
############################################

Deephaven natively supports Pyarrow tables. This example converts between a Pyarrow table and a Deephaven table:

    import pyarrow
    from pydeephaven import Session

    session = Session()

    arr = pyarrow.array([4,5,6], type=pyarrow.int32())
    pyarrow_table = pyarrow.Table.from_arrays([arr], names=["Integers"])

    table = session.import_table(pyarrow_table)
    session.bind_table(name="MyTable", table=table)

    #Convert the Deephaven table back to a pyarrow table
    pyarrow_table = table.snapshot()

Execute a script server side
############################

`session.run_script()` can be used to execute code on the Deephaven server. This is useful when operations cannot be done on the client-side, such as creating a dynamic table writer. This example shows how to execute a script server-side:

    from pydeephaven import Session

    session = Session()

    script = """
    from deephaven.TableTools import emptyTable

    table = emptyTable(8).update("Index = i")
    """

    session.run_script(script)

Error handling
##############

The `DHError` is thrown whenever the client package encounters an error. This example shows how to catch a `DHError`:

    from pydeephaven import Session, DHError

    try:
        session = Session(host="invalid_host")
    except DHError as e:
        print("Deephaven error when connecting to session")
        print(e)
    except Exception as e:
        print("Unknown error")
        print(e)
