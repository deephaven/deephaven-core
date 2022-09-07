/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "test_util.h"
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::TableHandle;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::valueOrThrow;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;
using deephaven::client::utility::TableMaker;

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

TableMakerForTests TableMakerForTests::create() {
  std::string connectionString("localhost:10000");
  streamf(std::cerr, "Connecting to %o\n", connectionString);
  auto client = Client::connect("localhost:10000");
  auto manager = client.getManager();

  ColumnNamesForTests cn;
  ColumnDataForTests cd;

  TableMaker maker;
  maker.addColumn(cn.importDate(), cd.importDate());
  maker.addColumn(cn.ticker(), cd.ticker());
  maker.addColumn(cn.open(), cd.open());
  maker.addColumn(cn.close(), cd.close());
  maker.addColumn(cn.volume(), cd.volume());

  auto testTable = maker.makeTable(manager);
  return TableMakerForTests(std::move(client), std::move(testTable), std::move(cn), std::move(cd));
}

TableMakerForTests::TableMakerForTests(TableMakerForTests &&) noexcept = default;
TableMakerForTests &TableMakerForTests::operator=(TableMakerForTests &&) noexcept = default;
TableMakerForTests::~TableMakerForTests() = default;


namespace internal {
void compareTableHelper(int depth, const std::shared_ptr<arrow::Table> &table,
    const std::string &columnName, const std::shared_ptr<arrow::Array> &data) {
  auto field = table->field(depth);
  auto column = table->column(depth);

  if (field->name() != columnName) {
    auto message = stringf("Column %o: Expected column name %o, have %o", depth, columnName,
        field->name());
    throw std::runtime_error(message);
  }

  arrow::ChunkedArray chunkedData(data);
  if (column->Equals(chunkedData)) {
    return;
  }

  if (column->length() != chunkedData.length()) {
    auto message = stringf("Column %o: Expected length %o, got %o", depth, chunkedData.length(),
        column->length());
    throw std::runtime_error(message);
  }

  if (!column->type()->Equals(chunkedData.type())) {
    auto message = stringf("Column %o: Expected type %o, got %o", depth,
        chunkedData.type()->ToString(), column->type()->ToString());
    throw std::runtime_error(message);
  }

  int64_t elementIndex = 0;
  int lChunkNum = 0;
  int rChunkNum = 0;
  int lChunkIndex = 0;
  int rChunkIndex = 0;
  while (elementIndex < column->length()) {
    if (lChunkNum >= column->num_chunks() || rChunkNum >= chunkedData.num_chunks()) {
      throw std::runtime_error("sad");
    }
    const auto &lChunk = column->chunk(lChunkNum);
    if (lChunkIndex == lChunk->length()) {
      lChunkIndex = 0;
      ++lChunkNum;
      continue;
    }

    const auto &rChunk = chunkedData.chunk(rChunkNum);
    if (rChunkIndex == rChunk->length()) {
      rChunkIndex = 0;
      ++rChunkNum;
      continue;
    }

    const auto lItem = valueOrThrow(DEEPHAVEN_EXPR_MSG(lChunk->GetScalar(lChunkIndex)));
    const auto rItem = valueOrThrow(DEEPHAVEN_EXPR_MSG(rChunk->GetScalar(rChunkIndex)));

    if (!lItem->Equals(rItem)) {
      auto message = stringf("Column %o: Columns differ at element %o: %o vs %o",
          depth, elementIndex, lItem->ToString(), rItem->ToString());
      throw std::runtime_error(message);
    }

    ++elementIndex;
    ++lChunkIndex;
    ++rChunkIndex;
  }

  throw std::runtime_error("Some other difference (TODO(kosak): describe difference)");
}

std::shared_ptr<arrow::Table> basicValidate(const TableHandle &table, int expectedColumns) {
  auto fsr = table.getFlightStreamReader();
  std::shared_ptr<arrow::Table> arrowTable;
  okOrThrow(DEEPHAVEN_EXPR_MSG(fsr->ReadAll(&arrowTable)));

  if (expectedColumns != arrowTable->num_columns()) {
    auto message = stringf("Expected %o columns, but table actually has %o columns",
        expectedColumns, arrowTable->num_columns());
    throw std::runtime_error(message);
  }

  return arrowTable;
}
}  // namespace internal
}  // namespace deephaven::client::tests
