/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "test_util.h"
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/dhcore/utility/utility.h"
#include <cstdlib>

using deephaven::client::TableHandle;
using deephaven::client::utility::OkOrThrow;
using deephaven::client::utility::TableMaker;
using deephaven::client::utility::ValueOrThrow;
using deephaven::dhcore::utility::Streamf;
using deephaven::dhcore::utility::Stringf;

namespace deephaven::client::tests {
ColumnNamesForTests::ColumnNamesForTests() : importDate_("ImportDate"), ticker_("Ticker"),
  open_("Open"), close_("Close"), volume_("Volume") {}
ColumnNamesForTests::ColumnNamesForTests(ColumnNamesForTests &&other) noexcept = default;
ColumnNamesForTests &ColumnNamesForTests::operator=(ColumnNamesForTests &&other) noexcept = default;
ColumnNamesForTests::~ColumnNamesForTests() = default;

ColumnDataForTests::ColumnDataForTests() {
  importDate_ = std::vector<std::string> {
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-02",
    };

  ticker_ = std::vector<std::string> {
    "XRX",
    "XRX",
    "XYZZY",
    "IBM",
    "GME",
    "AAPL",
    "AAPL",
    "AAPL",
    "ZNGA",
    "ZNGA",
    "T",
    };

  open_ = std::vector<double> {
    83.1,
    50.5,
    92.3,
    40.1,
    681.43,
    22.1,
    26.8,
    31.5,
    541.2,
    685.3,
    18.8
  };

  close_ = std::vector<double> {
    88.2,
    53.8,
    88.5,
    38.7,
    453,
    23.5,
    24.2,
    26.7,
    538.2,
    544.9,
    13.4
  };

  volume_ = std::vector<int64_t> {
    345000,
    87000,
    6060842,
    138000,
    138000000,
    100000,
    250000,
    19000,
    46123,
    48300,
    1500
  };
}

ColumnDataForTests::ColumnDataForTests(ColumnDataForTests &&other) noexcept = default;
ColumnDataForTests &ColumnDataForTests::operator=(ColumnDataForTests &&other) noexcept = default;
ColumnDataForTests::~ColumnDataForTests() = default;

TableMakerForTests TableMakerForTests::Create() {
  auto client = CreateClient();
  auto manager = client.GetManager();

  ColumnNamesForTests cn;
  ColumnDataForTests cd;

  TableMaker maker;
  maker.AddColumn(cn.ImportDate(), cd.ImportDate());
  maker.AddColumn(cn.Ticker(), cd.Ticker());
  maker.AddColumn(cn.Open(), cd.Open());
  maker.AddColumn(cn.Close(), cd.Close());
  maker.AddColumn(cn.Volume(), cd.Volume());

  auto test_table = maker.MakeTable(manager);
  return TableMakerForTests(std::move(client), std::move(test_table), std::move(cn), std::move(cd));
}

Client TableMakerForTests::CreateClient(const ClientOptions &options) {
  const char *hostptr = std::getenv("DH_HOST");
  const char *portptr = std::getenv("DH_PORT");
  std::string host = (hostptr == nullptr) ? "localhost" : hostptr;
  std::string port = (portptr == nullptr) ? "10000" : portptr;
  std::string connection_string(host + ":" + port);
  Streamf(std::cerr, "Connecting to %o\n", connection_string);
  auto client = Client::Connect(connection_string, options);
  return client;
}

TableMakerForTests::TableMakerForTests(TableMakerForTests::ClientType &&client,
    TableHandle &&test_table, ColumnNamesForTests &&column_names, ColumnDataForTests &&columnData) :
    client_(std::move(client)),
    testTable_(std::move(test_table)), columnNames_(std::move(column_names)),
    columnData_(std::move(columnData)) {}

TableMakerForTests::TableMakerForTests(TableMakerForTests &&) noexcept = default;
TableMakerForTests &TableMakerForTests::operator=(TableMakerForTests &&) noexcept = default;
TableMakerForTests::~TableMakerForTests() = default;


namespace internal {
void CompareTableHelper(int depth, const std::shared_ptr<arrow::Table> &table,
    const std::string &column_name, const std::shared_ptr<arrow::Array> &data) {
  auto field = table->field(depth);
  auto column = table->column(depth);

  if (field->name() != column_name) {
    auto message = Stringf("Column %o: Expected column name %o, have %o", depth, column_name,
        field->name());
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }

  arrow::ChunkedArray chunked_data(data);
  if (column->Equals(chunked_data)) {
    return;
  }

  if (column->length() != chunked_data.length()) {
    auto message = Stringf("Column %o: Expected length %o, got %o", depth, chunked_data.length(),
        column->length());
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }

  if (!column->type()->Equals(chunked_data.type())) {
    auto message = Stringf("Column %o: Expected type %o, got %o", depth,
        chunked_data.type()->ToString(), column->type()->ToString());
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }

  int64_t element_index = 0;
  int l_chunk_num = 0;
  int r_chunk_num = 0;
  int l_chunk_index = 0;
  int r_chunk_index = 0;
  while (element_index < column->length()) {
    if (l_chunk_num >= column->num_chunks() || r_chunk_num >= chunked_data.num_chunks()) {
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG("Logic error"));
    }
    const auto &l_chunk = column->chunk(l_chunk_num);
    if (l_chunk_index == l_chunk->length()) {
      l_chunk_index = 0;
      ++l_chunk_num;
      continue;
    }

    const auto &r_chunk = chunked_data.chunk(r_chunk_num);
    if (r_chunk_index == r_chunk->length()) {
      r_chunk_index = 0;
      ++r_chunk_num;
      continue;
    }

    const auto l_item = ValueOrThrow(DEEPHAVEN_EXPR_MSG(l_chunk->GetScalar(l_chunk_index)));
    const auto r_item = ValueOrThrow(DEEPHAVEN_EXPR_MSG(r_chunk->GetScalar(r_chunk_index)));

    if (!l_item->Equals(r_item)) {
      auto message = Stringf("Column %o: Columns differ at element %o: %o vs %o",
          depth, element_index, l_item->ToString(), r_item->ToString());
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }

    ++element_index;
    ++l_chunk_index;
    ++r_chunk_index;
  }

  // TODO(kosak): describe difference
  throw std::runtime_error(DEEPHAVEN_DEBUG_MSG("Some other difference"));
}

std::shared_ptr<arrow::Table> BasicValidate(const deephaven::client::TableHandle &table, int expected_columns) {
  auto fsr = table.GetFlightStreamReader();
  std::shared_ptr<arrow::Table> arrow_table;
  OkOrThrow(DEEPHAVEN_EXPR_MSG(fsr->ReadAll(&arrow_table)));

  if (expected_columns != arrow_table->num_columns()) {
    auto message = Stringf("Expected %o columns, but Table actually has %o columns",
        expected_columns, arrow_table->num_columns());
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }

  return arrow_table;
}
}  // namespace internal
}  // namespace deephaven::client::tests
