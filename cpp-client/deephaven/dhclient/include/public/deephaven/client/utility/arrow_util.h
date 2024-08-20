/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <arrow/type.h>
#include <arrow/flight/types.h>

#include "deephaven/dhcore/clienttable/client_table.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::client::utility {
class ArrowUtil {
  using ElementTypeId = deephaven::dhcore::ElementTypeId;
  using FlightDescriptor = arrow::flight::FlightDescriptor;
  using Schema = deephaven::dhcore::clienttable::Schema;

public:
  /**
   * This class should not be instantiated.
   */
  ArrowUtil() = delete;

  static FlightDescriptor ConvertTicketToFlightDescriptor(const std::string &ticket);

  /**
   * Try to convert the Arrow DataType to an ElementTypeId.
   * @param must_succeed Requires the conversion to succeed. If must_succeed is set, the method
   *   will throw an exception rather than returning false.
   * @return If the conversion succeeded, a populated optional. Otherwise (if the conversion failed)
   *   and must_succeed is true, throws an exception. Otherwise (if the conversion failed
   *   and must_succeed is false), returns an unset optional.
   */
  static std::optional<ElementTypeId::Enum> GetElementTypeId(const arrow::DataType &data_type,
      bool must_succeed);

  /**
   * Convert an Arrow Schema into a Deephaven Schema
   * @param schema The arrow Schema
   * @return a Deephaven Schema
   */
  static std::shared_ptr<Schema> MakeDeephavenSchema(const arrow::Schema &schema);
};


/**
 * If status is OK, do nothing. Otherwise throw a runtime error with an informative message.
 * @param debug_info A DebugInfo object, typically as provided by DEEPHAVEN_LOCATION_EXPR.
 * @param status the arrow::Status
 */
void OkOrThrow(const deephaven::dhcore::utility::DebugInfo &debug_info, const arrow::Status &status);

/**
 * If result's status is OK, do nothing. Otherwise throw a runtime error with an informative message.
 * @param debug_info A DebugInfo object, typically as provided by DEEPHAVEN_LOCATION_EXPR.
 * @param result an arrow::Result
 */
template<typename T>
void OkOrThrow(const deephaven::dhcore::utility::DebugInfo &debug_info, const arrow::Result<T> &result) {
  OkOrThrow(debug_info, result.status());
}

/**
 * If result's internal status is OK, return result's contained value.
 * Otherwise throw a runtime error with an informative message.
 * @param debug_info A DebugInfo object, typically as provided by DEEPHAVEN_LOCATION_EXPR.
 * @param result The arrow::Result
 */
template<typename T>
T ValueOrThrow(const deephaven::dhcore::utility::DebugInfo &debug_info, arrow::Result<T> result) {
  OkOrThrow(debug_info, result.status());
  return result.ValueUnsafe();
}
}  // namespace deephaven::client::utility
