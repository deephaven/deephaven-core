/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/flight.h"
#include "deephaven/client/flight.h"
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::TableHandle;
using deephaven::client::utility::OkOrThrow;
using deephaven::client::utility::ValueOrThrow;
using deephaven::dhcore::utility::Stringf;

#include <memory>

namespace deephaven::client::utility {
TableMaker::TableMaker() = default;
TableMaker::~TableMaker() = default;

void TableMaker::FinishAddColumn(std::string name, internal::TypeConverter info) {
  auto kvMetadata = std::make_shared<arrow::KeyValueMetadata>();
  OkOrThrow(DEEPHAVEN_EXPR_MSG(kvMetadata->Set("deephaven:type", info.DeephavenType())));

  auto field = std::make_shared<arrow::Field>(std::move(name), std::move(info.DataType()), true,
      std::move(kvMetadata));
  OkOrThrow(DEEPHAVEN_EXPR_MSG(schemaBuilder_.AddField(field)));

  if (columns_.empty()) {
    numRows_ = info.Column()->length();
  } else if (numRows_ != info.Column()->length()) {
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(
        Stringf("Column sizes not consistent: expected %o, have %o", numRows_,
            info.Column()->length())));
  }

  columns_.push_back(std::move(info.Column()));
}

TableHandle TableMaker::MakeTable(const TableHandleManager &manager) {
  auto schema = ValueOrThrow(DEEPHAVEN_EXPR_MSG(schemaBuilder_.Finish()));

  auto wrapper = manager.CreateFlightWrapper();
  auto ticket = manager.NewTicket();
  auto flightDescriptor = ConvertTicketToFlightDescriptor(ticket);

  arrow::flight::FlightCallOptions options;
  wrapper.AddHeaders(&options);

  std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
  std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;
  OkOrThrow(DEEPHAVEN_EXPR_MSG(wrapper.FlightClient()->DoPut(options, flightDescriptor,
      schema, &fsw, &fmr)));
  auto batch = arrow::RecordBatch::Make(schema, numRows_, std::move(columns_));

  OkOrThrow(DEEPHAVEN_EXPR_MSG(fsw->WriteRecordBatch(*batch)));
  OkOrThrow(DEEPHAVEN_EXPR_MSG(fsw->DoneWriting()));

  std::shared_ptr<arrow::Buffer> buf;
  OkOrThrow(DEEPHAVEN_EXPR_MSG(fmr->ReadMetadata(&buf)));
  OkOrThrow(DEEPHAVEN_EXPR_MSG(fsw->Close()));
  return manager.MakeTableHandleFromTicket(std::move(ticket));
}

namespace internal {
TypeConverter::TypeConverter(std::shared_ptr<arrow::DataType> dataType,
    std::string deephavenType, std::shared_ptr<arrow::Array> column) :
    dataType_(std::move(dataType)), deephavenType_(std::move(deephavenType)),
    column_(std::move(column)) {}
    TypeConverter::~TypeConverter() = default;

const char * const TypeConverterTraits<char16_t>::kDeephavenTypeName = "char";
const char * const TypeConverterTraits<bool>::kDeephavenTypeName = "java.lang.Boolean";
const char * const TypeConverterTraits<int8_t>::kDeephavenTypeName = "byte";
const char * const TypeConverterTraits<int16_t>::kDeephavenTypeName = "short";
const char * const TypeConverterTraits<int32_t>::kDeephavenTypeName = "int";
const char * const TypeConverterTraits<int64_t>::kDeephavenTypeName = "long";
const char * const TypeConverterTraits<float>::kDeephavenTypeName = "float";
const char * const TypeConverterTraits<double>::kDeephavenTypeName = "double";
const char * const TypeConverterTraits<std::string>::kDeephavenTypeName = "java.lang.String";
}  // namespace internal
}  // namespace deephaven::client::utility
