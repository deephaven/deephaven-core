/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/client/client.h"
#include "deephaven/client/flight.h"
#include "deephaven/client/utility/table_maker.h"

using deephaven::client::NumCol;
using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;
using deephaven::client::utility::ConvertTicketToFlightDescriptor;
using deephaven::client::utility::OkOrThrow;
using deephaven::client::utility::TableMaker;
using deephaven::client::utility::ValueOrThrow;

namespace {
void Doit(const TableHandleManager &manager);
}  // namespace

// This example shows how to use the Arrow Flight client to make a simple table.
int main(int argc, char *argv[]) {
  const char *server = "localhost:10000";
  if (argc > 1) {
    if (argc != 2 || std::strcmp("-h", argv[1]) == 0) {
      std::cerr << "Usage: " << argv[0] << " [host:port]" << std::endl;
      std::exit(1);
    }
    server = argv[1];
  }

  try {
    auto client = Client::Connect(server);
    auto manager = client.GetManager();
    Doit(manager);
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
}

namespace {
void Doit(const TableHandleManager &manager) {
  // 1. Build schema
  arrow::SchemaBuilder schemaBuilder;

  // 2. Add "Symbol" column (type: string) to schema
  {
    auto symbolMetadata = std::make_shared<arrow::KeyValueMetadata>();
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(symbolMetadata->Set("deephaven:type", "java.lang.String")));
    auto symbolField = std::make_shared<arrow::Field>("Symbol",
        std::make_shared<arrow::StringType>(), true, std::move(symbolMetadata));
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(schemaBuilder.AddField(symbolField)));
  }

  // 3. Add "Price" column (type: double) to schema
  {
    auto priceMetadata = std::make_shared<arrow::KeyValueMetadata>();
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(priceMetadata->Set("deephaven:type", "double")));
    auto priceField = std::make_shared<arrow::Field>("Price",
        std::make_shared<arrow::DoubleType>(), true, std::move(priceMetadata));
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(schemaBuilder.AddField(priceField)));
  }

  // 4. Add "Volume" column (type: int32) to schema
  {
    auto volumeMetadata = std::make_shared<arrow::KeyValueMetadata>();
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(volumeMetadata->Set("deephaven:type", "int")));
    auto volumeField = std::make_shared<arrow::Field>("Volume",
        std::make_shared<arrow::Int32Type>(), true, std::move(volumeMetadata));
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(schemaBuilder.AddField(volumeField)));
  }

  // 4. Schema is done
  auto schema = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(schemaBuilder.Finish()));

  // 5. Prepare symbol, price, and volume data cells
  std::vector<std::string> symbols{"FB", "AAPL", "NFLX", "GOOG"};
  std::vector<double> prices{101.1, 102.2, 103.3, 104.4};
  std::vector<int32_t> volumes{1000, 2000, 3000, 4000};
  auto numRows = symbols.size();
  if (numRows != prices.size() || numRows != volumes.size()) {
    throw DEEPHAVEN_LOCATION_EXPR(std::runtime_error(DEEPHAVEN_LOCATION_STR("sizes don't match")));
  }

  // 6. Move data to Arrow column builders
  arrow::StringBuilder symbolBuilder;
  arrow::DoubleBuilder priceBuilder;
  arrow::Int32Builder volumeBuilder;
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(symbolBuilder.AppendValues(symbols)));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(priceBuilder.AppendValues(prices)));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(volumeBuilder.AppendValues(volumes)));

  auto symbolArray = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(symbolBuilder.Finish()));
  auto priceArray = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(priceBuilder.Finish()));
  auto volumeArray = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(volumeBuilder.Finish()));

  // 7. Get Arrow columns from builders
  std::vector<std::shared_ptr<arrow::Array>> columns = {
      std::move(symbolArray),
      std::move(priceArray),
      std::move(volumeArray)
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
  auto fd = deephaven::client::utility::ConvertTicketToFlightDescriptor(ticket);

  // 12. Perform the doPut
  std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
  std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(wrapper.FlightClient()->DoPut(options, fd, schema, &fsw, &fmr)));

  // 13. Make a RecordBatch containing both the schema and the data
  auto batch = arrow::RecordBatch::Make(schema, static_cast<std::int64_t>(numRows), std::move(columns));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(fsw->WriteRecordBatch(*batch)));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(fsw->DoneWriting()));

  // 14. Read back a metadata message (ignored), then close the Writer
  std::shared_ptr<arrow::Buffer> buf;
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(fmr->ReadMetadata(&buf)));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(fsw->Close()));

  // 15. Now that the table is ready, bind the ticket to a TableHandle.
  auto table = manager.MakeTableHandleFromTicket(ticket);

  // 16. Use Deephaven high level operations to fetch the table and print it
  std::cout << "table is:\n" << table.Stream(true) << std::endl;
}
}  // namespace
