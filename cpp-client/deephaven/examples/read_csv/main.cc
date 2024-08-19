/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
using deephaven::client::utility::ArrowUtil;
using deephaven::client::utility::OkOrThrow;
using deephaven::client::utility::ValueOrThrow;

namespace {
arrow::Status Doit(const TableHandleManager &manager, const std::string &csvfn);
}  // namespace

int main(int argc, char* argv[]) {
  const char *server = "localhost:10000";
  if (argc != 2 && argc != 3) {
    std::cerr << "Usage: " << argv[0] << " [host:port] filename\n";
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
      std::cerr << "Failed with status " << st << '\n';
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

  auto fd = ArrowUtil::ConvertTicketToFlightDescriptor(ticket);
  auto res = wrapper.FlightClient()->DoPut(options, fd, arrow_table->schema());
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res));

  const auto &src_columns = arrow_table->columns();
  const size_t ncols = src_columns.size();
  const int nchunks = src_columns[0]->num_chunks();
  std::vector<std::shared_ptr<arrow::Array>> dest_columns(ncols);
  for (int chunk_index = 0; chunk_index < nchunks; ++chunk_index) {
    for (size_t col_index = 0; col_index < ncols; ++col_index) {
      dest_columns[col_index] = src_columns[col_index]->chunk(chunk_index);
    }
    auto batch = arrow::RecordBatch::Make(arrow_table->schema(), dest_columns[0]->length(), dest_columns);
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->writer->WriteRecordBatch(*batch)));
  }

  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->writer->DoneWriting()));

  std::shared_ptr<arrow::Buffer> buf;
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->reader->ReadMetadata(&buf)));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->writer->Close()));

  auto table_handle = manager.MakeTableHandleFromTicket(ticket);
  std::cout << "table is:\n" << table_handle.Stream(true) << '\n';
  table_handle.BindToVariable("showme");
  return arrow::Status::OK();
}
}  // namespace
