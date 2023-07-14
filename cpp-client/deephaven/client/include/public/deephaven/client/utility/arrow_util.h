/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <arrow/type.h>
#include <arrow/flight/types.h>
#include <arrow/type.h>

#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::client::utility {
arrow::flight::FlightDescriptor convertTicketToFlightDescriptor(const std::string &ticket);

/**
 * If result's status is OK, do nothing. Otherwise throw a runtime error with an informative message.
 * @param debugInfo A DebugInfo object, typically as provided by DEEPHAVEN_EXPR_MSG.
 * @param result an arrow::Result
 */
template<typename T>
void okOrThrow(const deephaven::dhcore::utility::DebugInfo &debugInfo, const arrow::Result<T> &result) {
  okOrThrow(debugInfo, result.status());
}

/**
 * If status is OK, do nothing. Otherwise throw a runtime error with an informative message.
 * @param debugInfo A DebugInfo object, typically as provided by DEEPHAVEN_EXPR_MSG.
 * @param status the arrow::Status
 */
void okOrThrow(const deephaven::dhcore::utility::DebugInfo &debugInfo, const arrow::Status &status);

/**
 * If result's internal status is OK, return result's contained value.
 * Otherwise throw a runtime error with an informative message.
 * @param debugInfo A DebugInfo object, typically as provided by DEEPHAVEN_EXPR_MSG.
 * @param result The arrow::Result
 */
template<typename T>
T valueOrThrow(const deephaven::dhcore::utility::DebugInfo &debugInfo, arrow::Result<T> result) {
  okOrThrow(debugInfo, result.status());
  return result.ValueUnsafe();
}
}  // namespace deephaven::client::utility
