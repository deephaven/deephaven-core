/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <functional>
#include "deephaven/client/container/row_sequence.h"

namespace deephaven::client::subscription {
class ShiftProcessor {
  typedef deephaven::client::container::RowSequence RowSequence;
public:
  ShiftProcessor() = delete;  // static-only class

  static void applyShiftData(const RowSequence &firstIndex, const RowSequence &lastIndex,
      const RowSequence &destIndex,
      const std::function<void(uint64_t, uint64_t, uint64_t)> &processShift);
};
}  // namespace deephaven::client::subscription
