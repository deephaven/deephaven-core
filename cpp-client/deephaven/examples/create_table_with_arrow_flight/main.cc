/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/client/client.h"
#include "deephaven/client/flight.h"
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;
using deephaven::client::utility::ArrowUtil;
using deephaven::client::utility::OkOrThrow;
using deephaven::client::utility::TableMaker;
using deephaven::client::utility::ValueOrThrow;
using deephaven::dhcore::utility::GetWhat;

namespace {
void Doit(const TableHandleManager &manager);
}  // namespace

// This example shows how to use the Arrow Flight client to make a simple table.
int main(int argc, char *argv[]) {
  const char *server = "localhost:10000";
  if (argc > 1) {
    if (argc != 2 || std::strcmp("-h", argv[1]) == 0) {
      std::cerr << "Usage: " << argv[0] << " [host:port]\n";
      std::exit(1);
    }
    server = argv[1];
  }

  try {
    auto client = Client::Connect(server);
    auto manager = client.GetManager();
    Doit(manager);
  } catch (...) {
    std::cerr << "Caught exception: " << GetWhat(std::current_exception()) << '\n';
  }
}

namespace {
void Doit(const TableHandleManager &manager) {
  // 1. Build schema
  arrow::SchemaBuilder schema_builder;

  // 2. Add "Symbol" column (type: string) to schema
  {
    auto symbol_metadata = std::make_shared<arrow::KeyValueMetadata>();
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(symbol_metadata->Set("deephaven:type", "java.lang.String")));
    auto symbol_field = std::make_shared<arrow::Field>("Symbol",
        std::make_shared<arrow::StringType>(), true, std::move(symbol_metadata));
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(schema_builder.AddField(symbol_field)));
  }

  // 3. Add "Price" column (type: double) to schema
  {
    auto price_metadata = std::make_shared<arrow::KeyValueMetadata>();
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(price_metadata->Set("deephaven:type", "double")));
    auto price_field = std::make_shared<arrow::Field>("Price",
        std::make_shared<arrow::DoubleType>(), true, std::move(price_metadata));
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(schema_builder.AddField(price_field)));
  }

  // 4. Add "Volume" column (type: int32) to schema
  {
    auto volume_metadata = std::make_shared<arrow::KeyValueMetadata>();
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(volume_metadata->Set("deephaven:type", "int")));
    auto volume_field = std::make_shared<arrow::Field>("Volume",
        std::make_shared<arrow::Int32Type>(), true, std::move(volume_metadata));
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(schema_builder.AddField(volume_field)));
  }

  // 4. Schema is done
  auto schema = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(schema_builder.Finish()));

  // 5. Prepare symbol, price, and volume data cells
  std::vector<std::string> symbols{"FB", "AAPL", "NFLX", "GOOG"};
  std::vector<double> prices{101.1, 102.2, 103.3, 104.4};
  std::vector<int32_t> volumes{1000, 2000, 3000, 4000};
  auto num_rows = symbols.size();
  if (num_rows != prices.size() || num_rows != volumes.size()) {
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR("sizes don't match"));
  }

  // 6. Move data to Arrow column builders
  arrow::StringBuilder symbol_builder;
  arrow::DoubleBuilder price_builder;
  arrow::Int32Builder volume_builder;
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(symbol_builder.AppendValues(symbols)));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(price_builder.AppendValues(prices)));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(volume_builder.AppendValues(volumes)));

  auto symbol_array = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(symbol_builder.Finish()));
  auto price_array = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(price_builder.Finish()));
  auto volume_array = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(volume_builder.Finish()));

  // 7. Get Arrow columns from builders
  std::vector<std::shared_ptr<arrow::Array>> columns = {
      std::move(symbol_array),
      std::move(price_array),
      std::move(volume_array)
  };

  // 8. Get a Deephaven "FlightWrapper" object to access Arrow Flight
  auto wrapper = manager.CreateFlightWrapper();

  // 9. Allocate a Ticket to be used to reference the result
  auto ticket = manager.NewTicket();

  // 10. DoPut takes FlightCallOptions, which need to at least contain the Deephaven
  // authentication headers for this session.
  arrow::flight::FlightCallOptions options;
  wrapper.AddHeaders(&options);

  // 11. Make a FlightDescriptor from the ticket
  auto fd = ArrowUtil::ConvertTicketToFlightDescriptor(ticket);

  // 12. Perform the doPut
  auto res = wrapper.FlightClient()->DoPut(options, fd, schema);
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res));

  // 13. Make a RecordBatch containing both the schema and the data
  auto batch = arrow::RecordBatch::Make(schema, static_cast<std::int64_t>(num_rows), std::move(columns));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->writer->WriteRecordBatch(*batch)));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->writer->DoneWriting()));

  // 14. Read back a metadata message (ignored), then close the Writer
  std::shared_ptr<arrow::Buffer> buf;
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->reader->ReadMetadata(&buf)));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->writer->Close()));

  // 15. Now that the table is ready, bind the ticket to a TableHandle.
  auto table = manager.MakeTableHandleFromTicket(ticket);

  // 16. Use Deephaven high level operations to fetch the table and print it
  std::cout << "table is:\n" << table.Stream(true) << '\n';
}
}  // namespace
