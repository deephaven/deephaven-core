Creating a table with TableMaker
================================

The
:cpp:class:`TableMaker <deephaven::client::utility::TableMaker>`
is a utility class that helps simplify the task of creating small tables.
It is useful for prototyping and for creating small programs when you
don't want to concern yourself with the full power and flexibility
of ``arrow::flight::doPut()``.

Client programs that create tables using
:cpp:class:`TableMaker <deephaven::client::utility::TableMaker>`
typically follow the below recipe:

1. Default-construct a :cpp:class:`TableMaker <deephaven::client::utility::TableMaker>`
2. Add columns one a time with :cpp:func:`addColumn <deephaven::client::utility::TableMaker::addColumn()>`
3. Create the table with :cpp:func:`makeTable <deephaven::client::utility::TableMaker::makeTable()>`

Consider the following program from ``cpp-examples/make_table``:

.. code:: c++

  #include <iostream>
  #include "deephaven/client/highlevel/client.h"
  #include "deephaven/client/utility/table_maker.h"

  using deephaven::client::NumCol;
  using deephaven::client::Client;
  using deephaven::client::TableHandle;
  using deephaven::client::TableHandleManager;
  using deephaven::client::utility::TableMaker;

  // This example shows how to use the TableMaker wrapper to make a simple table.
  void doit(const TableHandleManager &manager) {
    TableMaker tm;
    std::vector<std::string> symbols{"FB", "AAPL", "NFLX", "GOOG"};
    std::vector<double> prices{101.1, 102.2, 103.3, 104.4};
    tm.addColumn("Symbol", symbols);
    tm.addColumn("Price", prices);
    auto table = tm.makeTable(manager, "myTable");

    std::cout << "table is:\n" << table.stream(true) << std::endl;
  }

  int main() {
    const char *server = "localhost:10000";
    auto client = Client::connect(server);
    auto manager = client.getManager();

    try {
      doit(manager);
    } catch (const std::runtime_error &e) {
      std::cerr << "Caught exception: " << e.what() << '\n';
    }
  }

This is the output of the program::

  table is:
  Symbol  Price
  FB      101.1
  AAPL    102.2
  NFLX    103.3
  GOOG    104.4

