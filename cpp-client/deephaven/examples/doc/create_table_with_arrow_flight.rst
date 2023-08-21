Creating a table with Arrow Flight
==================================

Client programs that create tables using
`Arrow Flight RPC <https://arrow.apache.org/docs/cpp/flight.html>`__
typically follow the below recipe:

1. Get a :cpp:class:`FlightWrapper <deephaven::client::FlightWrapper>` class via
   :cpp:func:`TableHandleManager::createFlightWrapper <deephaven::client::TableHandleManager::createFlightWrapper>`
2. For calls like Arrow ``DoPut`` that take an ``arrow::Flight::FlightCallOptions``, endow that object with
   Deephaven authentication headers via
   :cpp:func:`FlightWrapper::addAuthHeaders <deephaven::client::FlightWrapper::addAuthHeaders()>`
3. Get a pointer to the ``arrow::flight::FlightClient`` from
   :cpp:func:`FlightWrapper::flightClient <deephaven::client::FlightWrapper::flightClient()>`
4. Then perform the operations as described in
   `Arrow Flight RPC <https://arrow.apache.org/docs/cpp/flight.html>`__   

Consider the following program from ``cpp-examples/create_table_with_arrow_flight``:

.. code:: c++

  #include <iostream>
  #include "deephaven/client/highlevel/client.h"
  #include "deephaven/client/utility/table_maker.h"

  using deephaven::client::NumCol;
  using deephaven::client::Client;
  using deephaven::client::TableHandle;
  using deephaven::client::TableHandleManager;
  using deephaven::client::utility::flight::statusOrDie;
  using deephaven::client::utility::flight::valueOrDie;
  using deephaven::client::utility::TableMaker;

  // This example shows how to use the Arrow Flight client to make a simple table.
  void doit(const TableHandleManager &manager) {
    // 1. Build schema
    arrow::SchemaBuilder schemaBuilder;

    // 2. Add "Symbol" column (type: string) to schema
    auto symbolMetadata = std::make_shared<arrow::KeyValueMetadata>();
    statusOrDie(symbolMetadata->Set("deephaven:type", "java.lang.String"), "KeyValueMetadata::Set");
    auto symbolField = std::make_shared<arrow::Field>("Symbol",
	std::make_shared<arrow::StringType>(), true, std::move(symbolMetadata));
    statusOrDie(schemaBuilder.AddField(symbolField), "SchemaBuilder::AddField");

    // 3. Add "Price" column (type: double) to schema
    auto priceMetadata = std::make_shared<arrow::KeyValueMetadata>();
    statusOrDie(priceMetadata->Set("deephaven:type", "double"), "KeyValueMetadata::Set");
    auto priceField = std::make_shared<arrow::Field>("Price",
	std::make_shared<arrow::StringType>(), true, std::move(priceMetadata));
    statusOrDie(schemaBuilder.AddField(priceField), "SchemaBuilder::AddField");

    // 4. Schema is done
    auto schema = valueOrDie(schemaBuilder.Finish(), "Failed to create schema");

    // 5. Prepare symbol and price data
    std::vector<std::string> symbols{"FB", "AAPL", "NFLX", "GOOG"};
    std::vector<double> prices{101.1, 102.2, 103.3, 104.4};
    auto numRows = static_cast<int64_t>(symbols.size());
    if (numRows != prices.size()) {
      throw std::runtime_error("sizes don't match");
    }

    // 6. Move data to Arrow column builders
    arrow::StringBuilder symbolBuilder;
    arrow::DoubleBuilder priceBuilder;
    symbolBuilder.AppendValues(symbols);
    priceBuilder.AppendValues(prices);

    // 7. Get Arrow columns from builders
    std::vector<std::shared_ptr<arrow::Array>> columns = {
	valueOrDie(symbolBuilder.Finish(), "symbolBuilder.Finish()"),
	valueOrDie(priceBuilder.Finish(), "priceBuilder.Finish()")
    };

    // 8. Get a Deephaven "FlightWrapper" object to access Arrow Flight
    auto wrapper = manager.createFlightWrapper();

    // 9. Allocate a TableHandle and get its corresponding Arrow flight descriptor
    auto [table, fd] = manager.newTableHandleAndFlightDescriptor();

    // 10. DoPut takes FlightCallOptions, which need to at least contain the Deephaven
    // authentication headers for this session.
    arrow::flight::FlightCallOptions options;
    wrapper.addAuthHeaders(&options);

    // 11. Perform the doPut
    std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
    std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;
    statusOrDie(wrapper.flightClient()->DoPut(options, fd, schema, &fsw, &fmr), "DoPut failed");

    // 12. Make a RecordBatch containing both the schema and the data
    auto batch = arrow::RecordBatch::Make(schema, numRows, std::move(columns));
    statusOrDie(fsw->WriteRecordBatch(*batch), "WriteRecordBatch failed");
    statusOrDie(fsw->DoneWriting(), "DoneWriting failed");

    // 13. Read back a metadata message (ignored), then close the Writer
    std::shared_ptr<arrow::Buffer> buf;
    statusOrDie(fmr->ReadMetadata(&buf), "ReadMetadata failed");
    statusOrDie(fsw->Close(), "Close failed");

    // 14. Use Deephaven high level operations to fetch the table and print it
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
