/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/arrowutil/arrow_column_source.h"

#include <cstddef>
#include <stdexcept>
#include <arrow/type.h>

#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/core.h"

namespace deephaven::client::arrowutil {
namespace internal {
size_t ScaleFromUnit(arrow::TimeUnit::type unit) {
  switch (unit) {
    case arrow::TimeUnit::SECOND: return 1'000'000'000;
    case arrow::TimeUnit::MILLI: return 1'000'000;
    case arrow::TimeUnit::MICRO: return 1'000;
    case arrow::TimeUnit::NANO: return 1;
    default: {
      auto message = fmt::format("Unhandled arrow::TimeUnit {}", static_cast<size_t>(unit));
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
  }
}
}  // namespace internal
}  // namespace deephaven::client::arrowutil
