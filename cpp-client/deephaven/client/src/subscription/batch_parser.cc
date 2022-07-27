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
    const ColumnDefinitions &colDefs,
    size_t numBatches,
    bool allowInconsistentColumnSizes,
    arrow::flight::FlightStreamReader *fsr,
    arrow::flight::FlightStreamChunk *flightStreamChunk,
    const std::function<void(const std::vector<std::shared_ptr<arrow::Array>> &)> &callback) {
  auto colDefsSize = colDefs.vec().size();
  if (numBatches == 0) {
    return;
  }

  while (true) {
    const auto &srcCols = flightStreamChunk->data->columns();
    auto ncols = srcCols.size();
    if (ncols != colDefsSize) {
      throw std::runtime_error(stringf("Received %o columns, but my table has %o columns", ncols,
          colDefsSize));
    }

    if (!allowInconsistentColumnSizes) {
      auto numRows = srcCols[0]->length();
      for (size_t i = 1; i < ncols; ++i) {
        const auto &srcColArrow = *srcCols[i];
        // I think you do not want this check for the modify case. When you are parsing modify
        // messages, the columns may indeed be of different sizes.
        if (srcColArrow.length() != numRows) {
          auto message = stringf(
              "Inconsistent column lengths: Column 0 has %o rows, but column %o has %o rows",
              numRows, i, srcColArrow.length());
          throw std::runtime_error(message);
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
