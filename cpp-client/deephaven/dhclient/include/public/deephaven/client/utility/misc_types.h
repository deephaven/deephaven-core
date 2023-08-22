/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <chrono>
#include <string>
#include <variant>

namespace deephaven::client::utility {
/**
 * Allows the caller to specify time points either as std::chrono::time_point, nanoseconds since
 * the epoch, or as an ISO 8601 time string.
 */
using TimePointSpecifier = std::variant<std::chrono::system_clock::time_point, int64_t, std::string>;

/**
 * Allows the caller to specify durations either as any of the std::chrono::durations (which will
 * be auto-converted to nanoseconds), as nanoseconds since the epoch, or as an ISO 8601 duration
 * string.
 */
using DurationSpecifier = std::variant<std::chrono::nanoseconds, int64_t, std::string>;
} // namespace deephaven::client::utility
