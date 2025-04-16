/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdint>
#include <functional>
#include "deephaven/dhcore/container/row_sequence.h"

namespace deephaven::dhcore::subscription {
class ShiftProcessor {
  using RowSequence = deephaven::dhcore::container::RowSequence;
public:
  ShiftProcessor() = delete;  // static-only class

  static void ApplyShiftData(const RowSequence &first_index, const RowSequence &last_index,
      const RowSequence &dest_index,
      const std::function<void(uint64_t, uint64_t, uint64_t)> &process_shift);
};
}  // namespace deephaven::dhcore::subscription
