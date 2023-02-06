Reading Data with Arrow Flight
==============================

Client programs that read tables using
`Arrow Flight RPC <https://arrow.apache.org/docs/cpp/flight.html>`__
typically follow the below recipe:

1. Get a ``shared_ptr`` to a ``arrow::flight::FlightStreamReader`` via
   :cpp:func:`TableHandle::getFlightStreamReader <deephaven::client::TableHandle::getFlightStreamReader>`
2. Read the data using operations as described in
   `Arrow Flight RPC <https://arrow.apache.org/docs/cpp/flight.html>`__   

Consider the following program from ``cpp-examples/read_table_with_arrow_flight``:

.. code:: c++
	  
  #include <iostream>
  #include "deephaven/client/highlevel/client.h"
  #include "deephaven/client/utility/table_maker.h"
  #include "deephaven/client/utility/utility.h"

  using deephaven::client::NumCol;
  using deephaven::client::Client;
  using deephaven::client::TableHandle;
  using deephaven::client::TableHandleManager;
  using deephaven::client::utility::flight::statusOrDie;
  using deephaven::client::utility::TableMaker;

  TableHandle makeTable(const TableHandleManager &manager) {
    TableMaker tm;
    std::vector<std::string> symbols{"FB", "AAPL", "NFLX", "GOOG"};
    std::vector<double> prices{101.1, 102.2, 103.3, 104.4};
    tm.addColumn("Symbol", symbols);
    tm.addColumn("Price", prices);
    return tm.makeTable(manager, "myTable");
  }

  void dumpSymbolColumn(const TableHandle &tableHandle) {
    auto fsr = tableHandle.getFlightStreamReader();
    while (true) {
      arrow::flight::FlightStreamChunk chunk;
      statusOrDie(fsr->Next(&chunk), "FlightStreamReader::Next()");
      if (chunk.data == nullptr) {
	break;
      }

      auto symbolChunk = chunk.data->GetColumnByName("Symbol");
      if (symbolChunk == nullptr) {
	throw std::runtime_error("Symbol column not found");
      }
      auto priceChunk = chunk.data->GetColumnByName("Price");
      if (priceChunk == nullptr) {
	throw std::runtime_error("Price column not found");
      }

      auto symbolAsStringArray = std::dynamic_pointer_cast<arrow::StringArray>(symbolChunk);
      auto priceAsDoubleArray = std::dynamic_pointer_cast<arrow::DoubleArray>(priceChunk);
      if (symbolAsStringArray == nullptr) {
	throw std::runtime_error("symbolChunk was not an arrow::StringArray");
      }
      if (priceAsDoubleArray == nullptr) {
	throw std::runtime_error("priceChunk was not an arrow::DoubleArray");
      }

      if (symbolAsStringArray->length() != priceAsDoubleArray->length()) {
	throw std::runtime_error("Lengths differ");
      }

      for (int64_t i = 0; i < symbolAsStringArray->length(); ++i) {
	auto symbol = symbolAsStringArray->GetView(i);
	auto price = priceAsDoubleArray->Value(i);
	std::cout << symbol << ' ' << price << '\n';
      }
    }
  }

  int main() {
    const char *server = "localhost:10000";
    auto client = Client::connect(server);
    auto manager = client.getManager();

    try {
      auto table = makeTable(manager);
      dumpSymbolColumn(table);
    } catch (const std::runtime_error &e) {
      std::cerr << "Caught exception: " << e.what() << '\n';
    }
  }
