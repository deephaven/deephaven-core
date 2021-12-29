/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::highlevel::TableHandle;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::valueOrThrow;

#include <memory>

namespace deephaven::client::utility {
TableMaker::TableMaker() = default;
TableMaker::~TableMaker() = default;

void TableMaker::finishAddColumn(std::string name, internal::TypeConverter info) {
  auto kvMetadata = std::make_shared<arrow::KeyValueMetadata>();
  okOrThrow(DEEPHAVEN_EXPR_MSG(kvMetadata->Set("deephaven:type", info.deephavenType())));

  auto field = std::make_shared<arrow::Field>(std::move(name), std::move(info.dataType()), true,
      std::move(kvMetadata));
  okOrThrow(DEEPHAVEN_EXPR_MSG(schemaBuilder_.AddField(field)));

  if (columns_.empty()) {
    numRows_ = info.column()->length();
  } else if (numRows_ != info.column()->length()) {
    auto message = stringf("Column sizes not consistent: expected %o, have %o", numRows_,
        info.column()->length());
    throw std::runtime_error(message);
  }

  columns_.push_back(std::move(info.column()));
}

TableHandle TableMaker::makeTable(const TableHandleManager &manager) {
  auto schema = valueOrThrow(DEEPHAVEN_EXPR_MSG(schemaBuilder_.Finish()));

  auto wrapper = manager.createFlightWrapper();
  auto [result, fd] = manager.newTableHandleAndFlightDescriptor();

  arrow::flight::FlightCallOptions options;
  wrapper.addAuthHeaders(&options);

  std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
  std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;
  okOrThrow(DEEPHAVEN_EXPR_MSG(wrapper.flightClient()->DoPut(options, fd, schema, &fsw, &fmr)));
  auto batch = arrow::RecordBatch::Make(schema, numRows_, std::move(columns_));

  okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->WriteRecordBatch(*batch)));
  okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->DoneWriting()));

  std::shared_ptr<arrow::Buffer> buf;
  okOrThrow(DEEPHAVEN_EXPR_MSG(fmr->ReadMetadata(&buf)));
  okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->Close()));
  return result;
}

namespace internal {
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
}  // namespace deephaven::client::utility
