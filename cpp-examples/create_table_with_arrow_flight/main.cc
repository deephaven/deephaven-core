/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/client/client.h"
#include "deephaven/client/utility/table_maker.h"

using deephaven::client::NumCol;
using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::valueOrThrow;
using deephaven::client::utility::TableMaker;

namespace {
void doit(const TableHandleManager &manager);
}  // namespace

// This example shows how to use the Arrow Flight client to make a simple table.
int main() {
  const char *server = "localhost:10000";

  try {
    auto client = Client::connect(server);
    auto manager = client.getManager();
    doit(manager);
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
}

namespace {
void doit(const TableHandleManager &manager) {
  // 1. Build schema
  arrow::SchemaBuilder schemaBuilder;

  // 2. Add "Symbol" column (type: string) to schema
  {
    auto symbolMetadata = std::make_shared<arrow::KeyValueMetadata>();
    okOrThrow(DEEPHAVEN_EXPR_MSG(symbolMetadata->Set("deephaven:type", "java.lang.String")));
    auto symbolField = std::make_shared<arrow::Field>("Symbol",
        std::make_shared<arrow::StringType>(), true, std::move(symbolMetadata));
    okOrThrow(DEEPHAVEN_EXPR_MSG(schemaBuilder.AddField(symbolField)));
  }

  // 3. Add "Price" column (type: double) to schema
  {
    auto priceMetadata = std::make_shared<arrow::KeyValueMetadata>();
    okOrThrow(DEEPHAVEN_EXPR_MSG(priceMetadata->Set("deephaven:type", "double")));
    auto priceField = std::make_shared<arrow::Field>("Price",
        std::make_shared<arrow::DoubleType>(), true, std::move(priceMetadata));
    okOrThrow(DEEPHAVEN_EXPR_MSG(schemaBuilder.AddField(priceField)));
  }

  // 4. Add "Volume" column (type: int32) to schema
  {
    auto volumeMetadata = std::make_shared<arrow::KeyValueMetadata>();
    okOrThrow(DEEPHAVEN_EXPR_MSG(volumeMetadata->Set("deephaven:type", "int")));
    auto volumeField = std::make_shared<arrow::Field>("Volume",
        std::make_shared<arrow::Int32Type>(), true, std::move(volumeMetadata));
    okOrThrow(DEEPHAVEN_EXPR_MSG(schemaBuilder.AddField(volumeField)));
  }

  // 4. Schema is done
  auto schema = valueOrThrow(DEEPHAVEN_EXPR_MSG(schemaBuilder.Finish()));

  // 5. Prepare symbol, price, and volume data cells
  std::vector<std::string> symbols{"FB", "AAPL", "NFLX", "GOOG"};
  std::vector<double> prices{101.1, 102.2, 103.3, 104.4};
  std::vector<int32_t> volumes{1000, 2000, 3000, 4000};
  auto numRows = static_cast<int64_t>(symbols.size());
  if (numRows != prices.size() || numRows != volumes.size()) {
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG("sizes don't match"));
  }

  // 6. Move data to Arrow column builders
  arrow::StringBuilder symbolBuilder;
  arrow::DoubleBuilder priceBuilder;
  arrow::Int32Builder volumeBuilder;
  okOrThrow(DEEPHAVEN_EXPR_MSG(symbolBuilder.AppendValues(symbols)));
  okOrThrow(DEEPHAVEN_EXPR_MSG(priceBuilder.AppendValues(prices)));
  okOrThrow(DEEPHAVEN_EXPR_MSG(volumeBuilder.AppendValues(volumes)));

  auto symbolArray = valueOrThrow(DEEPHAVEN_EXPR_MSG(symbolBuilder.Finish()));
  auto priceArray = valueOrThrow(DEEPHAVEN_EXPR_MSG(priceBuilder.Finish()));
  auto volumeArray = valueOrThrow(DEEPHAVEN_EXPR_MSG(volumeBuilder.Finish()));

  // 7. Get Arrow columns from builders
  std::vector<std::shared_ptr<arrow::Array>> columns = {
      std::move(symbolArray),
      std::move(priceArray),
      std::move(volumeArray)
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
  okOrThrow(DEEPHAVEN_EXPR_MSG(wrapper.flightClient()->DoPut(options, fd, schema, &fsw, &fmr)));

  // 12. Make a RecordBatch containing both the schema and the data
  auto batch = arrow::RecordBatch::Make(schema, numRows, std::move(columns));
  okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->WriteRecordBatch(*batch)));
  okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->DoneWriting()));

  // 13. Read back a metadata message (ignored), then close the Writer
  std::shared_ptr<arrow::Buffer> buf;
  okOrThrow(DEEPHAVEN_EXPR_MSG(fmr->ReadMetadata(&buf)));
  okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->Close()));

  // 14. Use Deephaven high level operations to fetch the table and print it
  std::cout << "table is:\n" << table.stream(true) << std::endl;
}
}  // namespace
