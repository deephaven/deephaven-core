Resource Management
===================

Generally, the types in the system behave as value types and are reference
counted internally. This means that they can be freely copied, those copies
behave interchangeably, and any server-side resources are cleaned up when
the last shared owner of an underlying resource is destructed.

Consider the following program from the file ``cleanup/main.cc``:

.. code:: c++

  #include <iostream>
  #include "deephaven/client/highlevel/client.h"

  using deephaven::client::highlevel::NumCol;
  using deephaven::client::highlevel::Client;
  using deephaven::client::highlevel::TableHandle;
  using deephaven::client::highlevel::TableHandleManager;

  // This example shows explicit QueryTable cleanup using destructors/RAII.
  void doit(const TableHandleManager &manager) {
    auto table = manager.emptyTable(10).update("X = ii % 2", "Y = ii");
    auto [x, y] = table.getCols<NumCol, NumCol>("X", "Y");
    // This example will dispose each table individually.

    auto t1 = table.where(y < 5);
    std::cerr << "This is t1:\n" << t1.stream(true) << '\n';

    {
      TableHandle t2Copy;
      {
	auto t2 = t1.countBy(x);
	std::cerr << "This is t2:\n" << t2.stream(true) << '\n';

	t2Copy = t2;

	// The variable 't2' will be destructed here, but the server resource will stay alive
	// because 't2Copy' is still live.
      }
      std::cerr << "t2Copy still alive:\n" << t2Copy.stream(true) << '\n';

      // t2Copy will be destructed here. As it is the last owner of the server resource,
      // the server resource will be released here.
    }

    // t1 and the TableHandleManger will be destructed here.
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

At the end of the inner block for ``doit()``, the destructor for ``t2`` runs.
However, the corresponding server resource is not yet released, because
``t2Copy`` is still live. At the end of the middle block, the destructor for
``t2Copy`` runs, and this will actually release the server resource. At the
end of the outer block, the destructor for ``t1`` runs and its resources
are released. Finally at the end of ``main()`` the destructor for ``client``
runs, the client is shut down, and the program terminates.
