/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdlib>
#include <arrow/flight/client.h>
#include "deephaven/client/utility/misc.h"

namespace deephaven::client::subscription {
class BatchParser {
  typedef deephaven::client::utility::ColumnDefinitions ColumnDefinitions;
public:

  BatchParser() = delete;

  static void parseBatches(
      size_t expectedNumCols,
      size_t numBatches,
      bool allowInconsistentColumnSizes,
      arrow::flight::FlightStreamReader *fsr,
      arrow::flight::FlightStreamChunk *flightStreamChunk,
      const std::function<void(const std::vector<std::shared_ptr<arrow::Array>> &)> &callback);
};
}  // namespace deephaven::client::subscription
