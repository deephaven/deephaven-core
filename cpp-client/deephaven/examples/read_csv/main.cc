/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <cstdlib>
#include <iostream>

#include "arrow/table.h"
#include "arrow/array.h"
#include "arrow/io/file.h"
#include "arrow/csv/api.h"
#include "arrow/status.h"
#include "arrow/pretty_print.h"
#include "deephaven/client/client.h"
#include "deephaven/client/flight.h"
#include "deephaven/client/utility/arrow_util.h"

using deephaven::client::TableHandleManager;
using deephaven::client::Client;
using deephaven::client::utility::ConvertTicketToFlightDescriptor;
using deephaven::client::utility::OkOrThrow;
using deephaven::client::utility::ValueOrThrow;

namespace {
arrow::Status Doit(const TableHandleManager &manager, const std::string &csvfn);
}  // namespace

int main(int argc, char* argv[]) {
  const char *server = "localhost:10000";
  if (argc != 2 && argc != 3) {
    std::cerr << "Usage: " << argv[0] << " [host:port] filename" << std::endl;
    std::exit(1);
  }
  int c = 1;
  if (argc == 3) {
    server = argv[c++];
  }
  const char *filename = argv[c++];

  try {
    auto client = Client::Connect(server);
    auto manager = client.GetManager();
    auto st = Doit(manager, filename);
    if (!st.ok()) {
      std::cerr << "Failed with status " << st << std::endl;
    }
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }

  return 0;
}

namespace {
arrow::Status Doit(const TableHandleManager &manager, const std::string &csvfn) {
  auto input_file = ValueOrThrow(
      DEEPHAVEN_LOCATION_EXPR(arrow::io::ReadableFile::Open(csvfn)));
  auto csv_reader = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(
     arrow::csv::TableReader::Make(
       arrow::io::default_io_context(),
       input_file,
       arrow::csv::ReadOptions::Defaults(),
       arrow::csv::ParseOptions::Defaults(),
       arrow::csv::ConvertOptions::Defaults()
    )
  ));
    
  auto arrow_table = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(csv_reader->Read()));

  auto wrapper = manager.CreateFlightWrapper();

  auto ticket = manager.NewTicket();

  arrow::flight::FlightCallOptions options;
  wrapper.AddHeaders(&options);

  auto fd = ConvertTicketToFlightDescriptor(ticket);
  std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
  std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(
      wrapper.FlightClient()->DoPut(options, fd, arrow_table->schema(), &fsw, &fmr)));

  const auto &srcColumns = arrow_table->columns();
  const size_t ncols = srcColumns.size();
  const size_t nchunks = srcColumns[0]->num_chunks();
  std::vector<std::shared_ptr<arrow::Array>> destColumns(ncols);
  for (size_t chunkIndex = 0; chunkIndex < nchunks; ++chunkIndex) {
    for (size_t colIndex = 0; colIndex < ncols; ++colIndex) {
      destColumns[colIndex] = srcColumns[colIndex]->chunk(chunkIndex);
    }
    auto batch = arrow::RecordBatch::Make(arrow_table->schema(), destColumns[0]->length(), destColumns);
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(fsw->WriteRecordBatch(*batch)));
  }

  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(fsw->DoneWriting()));

  std::shared_ptr<arrow::Buffer> buf;
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(fmr->ReadMetadata(&buf)));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(fsw->Close()));

  auto table_handle = manager.MakeTableHandleFromTicket(ticket);
  std::cout << "table is:\n" << table_handle.Stream(true) << std::endl;
  table_handle.BindToVariable("showme");
  return arrow::Status::OK();
}
}  // namespace
