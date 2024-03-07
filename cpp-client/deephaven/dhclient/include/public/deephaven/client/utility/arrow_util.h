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

#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::client::utility {
arrow::flight::FlightDescriptor ConvertTicketToFlightDescriptor(const std::string &ticket);

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
