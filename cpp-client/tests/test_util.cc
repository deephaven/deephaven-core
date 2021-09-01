#include "tests/test_util.h"

namespace deephaven {
namespace client {
namespace tests {

using deephaven::client::highlevel::TableHandle;
using deephaven::client::utility::flight::statusOrDie;
using deephaven::client::utility::flight::valueOrDie;
using deephaven::client::utility::stringf;

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

TableWizard::TableWizard() = default;
TableWizard::~TableWizard() = default;

void TableWizard::finishAddColumn(std::string name, internal::TypeConverter info) {
  auto kvMetadata = std::make_shared<arrow::KeyValueMetadata>();
  statusOrDie(kvMetadata->Set("deephaven:type", info.deephavenType()), "KeyValueMetadata::Set");

  auto field = std::make_shared<arrow::Field>(std::move(name), std::move(info.dataType()), true,
      std::move(kvMetadata));
  statusOrDie(schemaBuilder_.AddField(field), "SchemaBuilder::AddField");

  if (columns_.empty()) {
    numRows_ = info.column()->length();
  } else if (numRows_ != info.column()->length()) {
    auto message = stringf("Column sizes not consistent: expected %o, have %o", numRows_,
        info.column()->length());
    throw std::runtime_error(message);
  }

  columns_.push_back(std::move(info.column()));
}

TableMakerForTests TableMakerForTests::create() {
  std::cerr << "Connecting to server (TODO(kosak): parameterize connection name)\n";
  auto client = Client::connect("localhost:10000");
  auto manager = client.getManager();

  ColumnNamesForTests cn;
  ColumnDataForTests cd;

  TableWizard wizard;
  wizard.addColumn(cn.importDate(), cd.importDate());
  wizard.addColumn(cn.ticker(), cd.ticker());
  wizard.addColumn(cn.open(), cd.open());
  wizard.addColumn(cn.close(), cd.close());
  wizard.addColumn(cn.volume(), cd.volume());

  std::string testTableName = "demo";

  auto testTable = wizard.makeTable(manager, testTableName);

  return TableMakerForTests(std::move(client), std::move(testTable), std::move(testTableName),
      std::move(cn), std::move(cd));
}

TableMakerForTests::TableMakerForTests(TableMakerForTests &&) noexcept = default;
TableMakerForTests &TableMakerForTests::operator=(TableMakerForTests &&) noexcept = default;
TableMakerForTests::~TableMakerForTests() = default;

TableHandle TableWizard::makeTable(const TableHandleManager &manager, std::string tableName) {
  auto schema = valueOrDie(schemaBuilder_.Finish(), "Failed to create schema");

  auto wrapper = manager.createFlightWrapper();

  arrow::flight::FlightCallOptions options;
  wrapper.addAuthHeaders(&options);

  auto fd = arrow::flight::FlightDescriptor::Path({"scope", tableName});

  std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
  std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;
  statusOrDie(wrapper.flightClient()->DoPut(options, fd, schema, &fsw, &fmr), "DoPut failed");
  auto batch = arrow::RecordBatch::Make(schema, numRows_, std::move(columns_));

  statusOrDie(fsw->WriteRecordBatch(*batch), "WriteRecordBatch failed");
  statusOrDie(fsw->DoneWriting(), "DoneWriting failed");

  std::shared_ptr<arrow::Buffer> buf;
  statusOrDie(fmr->ReadMetadata(&buf), "ReadMetadata failed");
  statusOrDie(fsw->Close(), "Close failed");

  return manager.fetchTable(std::move(tableName));
}

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

    const auto lItem = valueOrDie(lChunk->GetScalar(lChunkIndex), "GetScalar");
    const auto rItem = valueOrDie(rChunk->GetScalar(rChunkIndex), "GetScalar");

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
  statusOrDie(fsr->ReadAll(&arrowTable), "FlightStreamReader::ReadAll");

  if (expectedColumns != arrowTable->num_columns()) {
    auto message = stringf("Expected %o columns, but table actually has %o columns",
        expectedColumns, arrowTable->num_columns());
    throw std::runtime_error(message);
  }

  return arrowTable;
}

TypeConverter::TypeConverter(std::shared_ptr<arrow::DataType> dataType,
    std::string deephavenType, std::shared_ptr<arrow::Array> column) :
    dataType_(std::move(dataType)), deephavenType_(std::move(deephavenType)),
    column_(std::move(column)) {}
TypeConverter::~TypeConverter() = default;

const char *TypeConverterTraits<bool>::deephavenTypeName = "java.lang.Boolean";
const char *TypeConverterTraits<int8_t>::deephavenTypeName = "byte";
const char *TypeConverterTraits<int16_t>::deephavenTypeName = "short";
const char *TypeConverterTraits<int32_t>::deephavenTypeName = "int";
const char *TypeConverterTraits<int64_t>::deephavenTypeName = "long";
const char *TypeConverterTraits<float>::deephavenTypeName = "float";
const char *TypeConverterTraits<double>::deephavenTypeName = "double";
const char *TypeConverterTraits<std::string>::deephavenTypeName = "java.lang.String";
}  // namespace internal

}  // namespace tests
}  // namespace client
}  // namespace deephaven
