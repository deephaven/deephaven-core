/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */
#include <cstdlib>
#include <iostream>

#include "arrow/table.h"
#include "arrow/array.h"
#include "arrow/io/file.h"
#include "arrow/csv/api.h"
#include "arrow/status.h"
#include "arrow/pretty_print.h"
#include "deephaven/client/highlevel/client.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::highlevel::TableHandleManager;
using deephaven::client::highlevel::Client;
using deephaven::client::utility::flight::statusOrDie;
using deephaven::client::utility::flight::valueOrDie;

namespace {

arrow::Status doit(const TableHandleManager &manager, const std::string csvfn) {
  arrow::io::IOContext io_context = arrow::io::default_io_context();
  auto input_file = valueOrDie(
    arrow::io::ReadableFile::Open(csvfn),
    "arrow::io::ReadableFile::Open(csvfn)"
  );
  auto csv_reader = valueOrDie(
     arrow::csv::TableReader::Make(
       arrow::io::default_io_context(),
       input_file,
       arrow::csv::ReadOptions::Defaults(),
       arrow::csv::ParseOptions::Defaults(),
       arrow::csv::ConvertOptions::Defaults()
    ),
    "arrow::csv::TableReader::Make"
  );
    
  auto arrow_table = valueOrDie(csv_reader->Read(), "csv_reader->Read()");

  auto wrapper = manager.createFlightWrapper();

  auto [table_handle, fd] = manager.newTableHandleAndFlightDescriptor();

  arrow::flight::FlightCallOptions options;
  wrapper.addAuthHeaders(&options);

  std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
  std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;
  statusOrDie(wrapper.flightClient()->DoPut(options, fd, arrow_table->schema(), &fsw, &fmr), "DoPut failed");

  auto chunks = arrow_table->columns();
  const size_t ncols = chunks.size();
  const size_t nchunks = chunks[0]->num_chunks();
  std::vector<std::shared_ptr<arrow::Array>> chunk(ncols);
  for (size_t i = 0; i < nchunks; ++i) {
    for (size_t j = 0; j < ncols; ++j) {
      chunk[j] = chunks[i]->chunk(j);
    }
    auto batch = arrow::RecordBatch::Make(arrow_table->schema(), chunk[0]->length(), chunk);
    statusOrDie(fsw->WriteRecordBatch(*batch), "WriteRecordBatch failed");
  }

  statusOrDie(fsw->DoneWriting(), "DoneWriting failed");

  std::shared_ptr<arrow::Buffer> buf;
  statusOrDie(fmr->ReadMetadata(&buf), "ReadMetadata failed");
  statusOrDie(fsw->Close(), "Close failed");

  std::cout << "table is:\n" << table_handle.stream(true) << std::endl;
  return arrow::Status::OK();
}

}  // namespace

int main(int argc, char* argv[]) {

  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " csv-fn" << std::endl;
    std::exit(1);
  }
  
  const char *server = "localhost:10000";
  auto client = Client::connect(server);
  auto manager = client.getManager();

  try {
    auto st = doit(manager, argv[1]);
    if (!st.ok()) {
      std::cerr << "Failed with status " << st << std::endl;
    }
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }

  return 0;
}
