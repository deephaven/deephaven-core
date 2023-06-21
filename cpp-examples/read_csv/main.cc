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
using deephaven::client::utility::convertTicketToFlightDescriptor;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::valueOrThrow;

namespace {
arrow::Status doit(const TableHandleManager &manager, const std::string &csvfn);
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
    auto client = Client::connect(server);
    auto manager = client.getManager();
    auto st = doit(manager, filename);
    if (!st.ok()) {
      std::cerr << "Failed with status " << st << std::endl;
    }
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }

  return 0;
}

namespace {
arrow::Status doit(const TableHandleManager &manager, const std::string &csvfn) {
  auto input_file = valueOrThrow(
      DEEPHAVEN_EXPR_MSG(arrow::io::ReadableFile::Open(csvfn)));
  auto csv_reader = valueOrThrow(DEEPHAVEN_EXPR_MSG(
     arrow::csv::TableReader::Make(
       arrow::io::default_io_context(),
       input_file,
       arrow::csv::ReadOptions::Defaults(),
       arrow::csv::ParseOptions::Defaults(),
       arrow::csv::ConvertOptions::Defaults()
    )
  ));
    
  auto arrow_table = valueOrThrow(DEEPHAVEN_EXPR_MSG(csv_reader->Read()));

  auto wrapper = manager.createFlightWrapper();

  auto ticket = manager.newTicket();

  arrow::flight::FlightCallOptions options;
  wrapper.addHeaders(&options);

  auto fd = convertTicketToFlightDescriptor(ticket);
  std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
  std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;
  okOrThrow(DEEPHAVEN_EXPR_MSG(
      wrapper.flightClient()->DoPut(options, fd, arrow_table->schema(), &fsw, &fmr)));

  const auto &srcColumns = arrow_table->columns();
  const size_t ncols = srcColumns.size();
  const int nchunks = srcColumns[0]->num_chunks();
  std::vector<std::shared_ptr<arrow::Array>> destColumns(ncols);
  for (int chunkIndex = 0; chunkIndex < nchunks; ++chunkIndex) {
    for (int colIndex = 0; colIndex < ncols; ++colIndex) {
      destColumns[colIndex] = srcColumns[colIndex]->chunk(chunkIndex);
    }
    auto batch = arrow::RecordBatch::Make(arrow_table->schema(), destColumns[0]->length(), destColumns);
    okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->WriteRecordBatch(*batch)));
  }

  okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->DoneWriting()));

  std::shared_ptr<arrow::Buffer> buf;
  okOrThrow(DEEPHAVEN_EXPR_MSG(fmr->ReadMetadata(&buf)));
  okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->Close()));

  auto table_handle = manager.makeTableHandleFromTicket(ticket);
  std::cout << "table is:\n" << table_handle.stream(true) << std::endl;
  table_handle.bindToVariable("showme");
  return arrow::Status::OK();
}
}  // namespace
