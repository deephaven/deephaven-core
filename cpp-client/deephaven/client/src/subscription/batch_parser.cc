/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/subscription/batch_parser.h"

#include <functional>
#include <arrow/array.h>
#include "deephaven/client/utility/utility.h"

using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::stringf;

namespace deephaven::client::subscription {
// Processes all of the adds in this add batch. Will invoke (numAdds - 1) additional calls to GetNext().
void BatchParser::parseBatches(
    size_t expectedNumCols,
    size_t numBatches,
    bool allowInconsistentColumnSizes,
    arrow::flight::FlightStreamReader *fsr,
    arrow::flight::FlightStreamChunk *flightStreamChunk,
    const std::function<void(const std::vector<std::shared_ptr<arrow::Array>> &)> &callback) {
  if (numBatches == 0) {
    return;
  }

  while (true) {
    const auto &srcCols = flightStreamChunk->data->columns();
    auto ncols = srcCols.size();
    if (ncols != expectedNumCols) {
      auto message = stringf("Expected %o columns, got %o", expectedNumCols, ncols);
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }

    if (!allowInconsistentColumnSizes) {
      auto numRows = srcCols[0]->length();
      for (size_t i = 1; i < ncols; ++i) {
        const auto &srcColArrow = *srcCols[i];
        if (srcColArrow.length() != numRows) {
          auto message = stringf(
              "Inconsistent column lengths: Column 0 has %o rows, but column %o has %o rows",
              numRows, i, srcColArrow.length());
          throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
        }
      }
    }

    callback(srcCols);

    if (--numBatches == 0) {
      return;
    }
    okOrThrow(DEEPHAVEN_EXPR_MSG(fsr->Next(flightStreamChunk)));
  }
}

}  // namespace deephaven::client::subscription
