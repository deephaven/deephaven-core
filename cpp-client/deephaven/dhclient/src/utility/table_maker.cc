/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/flight.h"
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/format.h"

using deephaven::client::TableHandle;
using deephaven::client::utility::OkOrThrow;
using deephaven::client::utility::ValueOrThrow;

#include <memory>

namespace deephaven::client::utility {
TableMaker::TableMaker() = default;
TableMaker::~TableMaker() = default;

void TableMaker::FinishAddColumn(std::string name, internal::TypeConverter info) {
  auto kv_metadata = std::make_shared<arrow::KeyValueMetadata>();
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(kv_metadata->Set("deephaven:type", info.DeephavenType())));

  auto field = std::make_shared<arrow::Field>(std::move(name), std::move(info.DataType()), true,
      std::move(kv_metadata));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(schemaBuilder_.AddField(field)));

  if (columns_.empty()) {
    numRows_ = info.Column()->length();
  } else if (numRows_ != info.Column()->length()) {
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(
        fmt::format("Column sizes not consistent: expected {}, have {}", numRows_,
            info.Column()->length())));
  }

  columns_.push_back(std::move(info.Column()));
}

TableHandle TableMaker::MakeTable(const TableHandleManager &manager) {
  auto schema = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(schemaBuilder_.Finish()));

  auto wrapper = manager.CreateFlightWrapper();
  auto ticket = manager.NewTicket();
  auto flight_descriptor = ArrowUtil::ConvertTicketToFlightDescriptor(ticket);

  arrow::flight::FlightCallOptions options;
  wrapper.AddHeaders(&options);

  auto res = wrapper.FlightClient()->DoPut(options, flight_descriptor, schema);
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res));
  auto batch = arrow::RecordBatch::Make(schema, numRows_, std::move(columns_));

  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->writer->WriteRecordBatch(*batch)));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->writer->DoneWriting()));

  std::shared_ptr<arrow::Buffer> buf;
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->reader->ReadMetadata(&buf)));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->writer->Close()));
  return manager.MakeTableHandleFromTicket(std::move(ticket));
}

namespace internal {
TypeConverter::TypeConverter(std::shared_ptr<arrow::DataType> data_type,
    std::string deephaven_type, std::shared_ptr<arrow::Array> column) :
    dataType_(std::move(data_type)), deephavenType_(std::move(deephaven_type)),
    column_(std::move(column)) {}
    TypeConverter::~TypeConverter() = default;
}  // namespace internal
}  // namespace deephaven::client::utility
