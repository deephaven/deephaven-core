/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <arrow/type.h>
#include "deephaven/client/utility/utility.h"

namespace deephaven::client::utility {
/**
 * If result's status is OK, do nothing. Otherwise throw a runtime error with an informative message.
 * @param debugInfo A DebugInfo object, typically as provided by DEEPHAVEN_EXPR_MESSAGE.
 * @param result an arrow::Result
 */
template<typename T>
void okOrThrow(const DebugInfo &debugInfo, const arrow::Result<T> &result) {
  okOrThrow(debugInfo, result.status());
}

/**
 * If status is OK, do nothing. Otherwise throw a runtime error with an informative message.
 * @param debugInfo A DebugInfo object, typically as provided by DEEPHAVEN_EXPR_MESSAGE.
 * @param status the arrow::Status
 */
void okOrThrow(const DebugInfo &debugInfo, const arrow::Status &status);

/**
 * If result's internal status is OK, return result's contained value.
 * Otherwise throw a runtime error with an informative message.
 * @param debugInfo A DebugInfo object, typically as provided by DEEPHAVEN_EXPR_MESSAGE.
 * @param result The arrow::Result
 */
template<typename T>
T valueOrThrow(const DebugInfo &debugInfo, arrow::Result<T> result) {
  okOrThrow(debugInfo, result.status());
  return result.ValueUnsafe();
}
}  // namespace deephaven::client::utility
