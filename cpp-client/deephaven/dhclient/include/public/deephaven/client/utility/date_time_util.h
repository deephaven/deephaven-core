/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <string_view>
#include "deephaven/dhcore/types.h"

namespace deephaven::client::utility {
class DateTimeUtil {
  using DateTime = deephaven::dhcore::DateTime;

public:
/**
 * Parses a string in ISO 8601 format into a DateTime.
 * @param iso_8601_timestamp The timestamp, in ISO 8601 format.
 * @return The corresponding DateTime.
 */
  static DateTime Parse(std::string_view iso_8601_timestamp);
};
}  // namespace deephaven::client::utility
