
Python Client Examples
######################

This page shows how to perform common operations with the Deephaven Python Client.

Initialize
##########

The
:py:class:`Session <pydeephaven.Session>`
class is your connection to Deephaven. This is what allows your Python code to interact with a Deephaven server::

    from pydeephaven import Session
    session = Session()

Binding to a table
##################

The
:py:class:`Session <pydeephaven.Session>`
class has many methods that create tables. This example creates a ticking time table and binds it to Deephaven::

    from pydeephaven import Session
    session = Session()
    table = session.time_table(period=1000000000).update(formulas=["Col1 = i % 2"])
    session.bind_table(name="my_table", table=table)

This is the general flow of how the Python client interacts with Deephaven. You create a table (new or existing), execute some operations on it, and then bind it to Deephaven. Binding the table gives it a named reference on the Deephaven server, so that it can be used from the Web API or other Sessions.

Execute a query on a table
##########################

:py:func:`table.update() <pydeephaven.table.Table.update>`
can be used to execute an update on a table. This updates a table with a query string::

    from pydeephaven import Session
    session = Session()

    # Create a table with no columns and 3 rows
    table = session.empty_table(3)

    # Create derived table having a new column MyColumn populated with the row index "i"
    table = table.update(["MyColumn = i"])

    # Update the Deephaven Web Console with this new table
    session.bind_table(name="my_table", table=table)

Sort a table
############

:py:func:`table.sort() <pydeephaven.table.Table.sort>`
can be used to sort a table. This example sorts a table by one of its columns::

    from pydeephaven import Session
    session = Session()
    table = session.empty_table(5)
    table = table.update(["SortColumn = 4-i"])
    table = table.sort(["SortColumn"])
    session.bind_table(name="my_table", table=table)

Filter a table
##############

:py:func:`table.where() <pydeephaven.table.Table.where>`
can be used to filter a table. This example filters a table using a filter string::

    from pydeephaven import Session
    session = Session()
    table = session.empty_table(5)
    table = table.update(["Values = i"])
    table = table.where(["Values % 2 == 1"])
    session.bind_table(name="my_table", table=table)

Query objects
#############

:py:class:`Query <pydeephaven.query.Query>`
objects are a way to create and manage a sequence of Deephaven query operations as a single unit.
:py:class:`Query <pydeephaven.query.Query>`
objects have the potential to perform better than the corresponding individual queries, because the
:py:class:`Query <pydeephaven.query.Query>`
object can be transmitted to the server in one request rather than several, and because the system can perform certain optimizations when it is able to see the whole sequence of queries at once. They are similar in spirit to prepared statements in SQL.

The general flow of using a
:py:class:`Query <pydeephaven.query.Query>`
object is to construct a query with a table, call the table operations
(:py:func:`sort <pydeephaven.table.Table.sort>`,
:py:func:`where <pydeephaven.table.Table.where>`,
:py:func:`update <pydeephaven.table.Table.update>`,
etc.) on the
:py:class:`Query <pydeephaven.query.Query>`
object, and then assign your table to the return value of
:py:func:`query.exec() <pydeephaven.query.Query.exec>`.

Any operation that can be executed on a table can also be executed on a
:py:class:`Query <pydeephaven.query.Query>`
object. This example shows two operations that compute the same result, with the first one using the table updates and the second one using a
:py:class:`Query <pydeephaven.query.Query>`
object::

    from pydeephaven import Session
    session = Session()
    table = session.empty_table(10)

    # executed immediately
    table1= table.update(["MyColumn = i"]).sort(["MyColumn"]).where(["MyColumn > 5"]);

    # create Query Object (execution is deferred until the "exec" statement)
    query_obj = session.query(table)
        .update(["MyColumn = i"])
        .sort(["MyColumn"])
        .where(["MyColumn > 5"]);

    # Transmit the QueryObject to the server and execute it
    table2 = query_obj.exec();

    session.bind_table(name="my_table1", table=table1)
    session.bind_table(name="my_table2", table=table2)

Join 2 tables
#############

:py:func:`table.join() <pydeephaven.table.Table.join>`
is one of many operations that can join two tables, as shown below::

    from pydeephaven import Session
    session = Session()
    table1 = session.empty_table(5)
    table1 = table1.update(["Values1 = i", "Group = i"])
    table2 = session.empty_table(5)
    table2 = table2.update(["Values2 = i + 10", "Group = i"])
    table = table1.join(table2, on=["Group"])
    session.bind_table(name="my_table", table=table)

Perform aggregations on a table
##################################

Aggregations can be applied on tables in the Python client. This example creates a aggregation that
averages the `Count` column of a table, and aggregates it by the `Group` column::

    from pydeephaven import Session, agg
    session = Session()
    table = session.empty_table(10)
    table = table.update(["Count = i", "Group = i % 2"])
    my_agg = agg.avg(["Count"])
    table = table.agg_by(aggs=[my_agg], by=["Group"])
    session.bind_table(name="my_table", table=table)

Convert a PyArrow table to a Deephaven table
############################################

Deephaven natively supports PyArrow tables. This example converts between a PyArrow table and a Deephaven table::

    import pyarrow as pa
    from pydeephaven import Session
    session = Session()
    arr = pa.array([4,5,6], type=pa.int32())
    pa_table = pa.Table.from_arrays([arr], names=["Integers"])
    table = session.import_table(pa_table)
    session.bind_table(name="my_table", table=table)
    #Convert the Deephaven table back to a pyarrow table
    pa_table = table.to_arrow()

Execute a script server side
############################

:py:func:`session.run_script() <pydeephaven.Session.run_script>` can be used to execute code on the Deephaven server. This is useful when operations cannot be done on the client-side, such as creating a dynamic table writer. This example shows how to execute a script server-side and retrieve a table generated from the script::

    from pydeephaven import Session
    session = Session()

    script = """
    from deephaven import empty_table
    table = empty_table(8).update(["Index = i"])
    """

    session.run_script(script)
    table = session.open_table("table")
    print(table.to_arrow())

Subscribe to a ticking table
############################

The `pydeephaven-ticking` package can be used to subscribe to ticking tables. This is useful for getting asynchronous callbacks when
they change. The package maintains a complete local copy of the table and notifies callers when the table changes.

Note that `pydeephaven-ticking` must be built before running this example. Build instructions are available `here <https://github.com/deephaven/deephaven-core/tree/main/py/client-ticking#readme>`__.

The listener can be specified either as a python function or as an implementation of the TableListener abstract base class. In the
case of implementing
:py:class:`TableListener <pydeephaven.ticking.TableListener>`
TableListener, the caller needs to implement
:py:func:`on_update <pydeephaven.ticking.TableListener.on_update>`
and optionally
:py:func:`on_error <pydeephaven.ticking.TableListener.on_error>`

as shown in the example::

    import time
    from pydeephaven import Session, TableListener, TableUpdate, listen

    session = Session()
    table = session.time_table(period=1000000000).update(formulas=["Col1 = i % 2"])

    class MyListener(TableListener):
        def on_update(self, update: TableUpdate) -> None:
            self._show_deltas("removes", update.removed())
            self._show_deltas("adds", update.added())
            self._show_deltas("modified-prev", update.modified_prev())
            self._show_deltas("modified", update.modified())

        def on_error(self, error: Exception):
            print(f"Error happened: {error}")

        def _show_deltas(self, what: str, dict: Dict[str, pa.Array]):
            if len(dict) == 0:
                return

            print(f"*** {what} ***")
            for name, data in dict.items():
                print(f"name={name}, data={data}")

    listen_handle = listen(table, MyListener())
    # Start processing data in another thread
    listen_handle.start()
    time.sleep(15)  # simulate doing other work for 15 seconds
    listen_handle.stop()

The
:py:func:`on_update <pydeephaven.ticking.table_listener.TableListener.on_update>`
callback method is invoked with a
:py:class:`TableUpdate <pydeephaven.ticking.table_listener.TableUpdate>` argument.
:py:class:`TableUpdate <pydeephaven_ticking.table_listener.TableUpdate>` argument.
:py:class:`TableUpdate <pydeephaven.ticking.table_listener.TableUpdate>`
has methods
`added()`,
`removed()`,
`modified_prev()`, and
`modified()`.
These methods return the data that was added, removed, or modified in this update.
`modified_prev()` returns the data as it was before the modify operation happened, whereas
`modified()` returns the modified data. This can be useful e.g. for calculations like keeping a running sum, where it is useful to know
the "old" value and the new value.

Each of the above methods has a "chunked" variant that returns a generator. This may be useful if the client is processing so much data
that it would like to handle it a chunk at a time. The chunked variants are `added_chunks()`, `removed_chunks()`, `modified_prev_chunks()`,
and `modified_chunks()`.

Error handling
##############

A
:py:class:`DHError <pydeephaven.dherror.DHError>`
is thrown whenever the client package encounters an error. This example shows how to catch a
:py:class:`DHError <pydeephaven.dherror.DHError>`::

    from pydeephaven import Session, DHError
    try:
        session = Session(host="invalid_host")
    except DHError as e:
        print("Deephaven error when connecting to session")
        print(e)
    except Exception as e:
        print("Unknown error")
        print(e)
