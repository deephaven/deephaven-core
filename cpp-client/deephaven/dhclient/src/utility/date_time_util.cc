/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */

#include "deephaven/client/utility/date_time_util.h"

#include <absl/time/time.h>
#include "deephaven/dhcore/types.h"
#define FMT_HEADER_ONLY
#include "fmt/core.h"

using deephaven::dhcore::DateTime;

namespace deephaven::client::utility {
DateTime DateTimeUtil::Parse(std::string_view iso_8601_timestamp) {
  constexpr const char *kFormatToUse = "%Y-%m-%dT%H:%M:%E*S%z";
  absl::Time result;
  std::string error_string;
  if (!absl::ParseTime(kFormatToUse, std::string(iso_8601_timestamp), &result, &error_string)) {
    auto message = fmt::format(R"x(Can't parse "{}" as ISO 8601 timestamp (using format string "{}"). Error is: {})x",
        iso_8601_timestamp, kFormatToUse, error_string);
    throw std::runtime_error(message);
  }
  auto nanos = absl::ToUnixNanos(result);
  return DateTime::FromNanos(nanos);
}
}  // namespace deephaven::client::utility
