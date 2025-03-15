/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <chrono>
#include <iomanip>
#include <sstream>

#include "deephaven/dhcore/types.h"

namespace deephaven::dhcore::utility {

template <typename Clock = std::chrono::system_clock, typename Duration = typename Clock::duration>
DateTime ToDateTime(const std::chrono::time_point<Clock, Duration> &tp) {
  using namespace std::chrono;
  return DateTime::FromNanos(duration_cast<nanoseconds>(tp.time_since_epoch()).count());
}

template <typename Clock = std::chrono::system_clock, typename Duration = typename Clock::duration>
std::chrono::time_point<Clock, Duration> ToTimePoint(const DateTime &dt) {
  using namespace std::chrono;
  const nanoseconds ns(dt.Nanos());
  return std::chrono::time_point<Clock, Duration>(ns);
}

std::string FormatDuration(std::chrono::nanoseconds nanos) {
  using namespace std::chrono;
  auto ns = nanos.count();
  auto h = duration_cast<hours>(nanos);
  ns -= duration_cast<nanoseconds>(h).count();
  auto m = duration_cast<minutes>(nanoseconds(ns));
  ns -= duration_cast<nanoseconds>(m).count();
  auto s = duration_cast<seconds>(nanoseconds(ns));
  ns -= duration_cast<nanoseconds>(s).count();
  auto ms = duration_cast<milliseconds>(nanoseconds(ns));

  std::stringstream ss;
  if (h.count() > 0) {
    ss << h.count() << 'h';
  }
  if (m.count() > 0 || h.count() > 0) {
    if (h.count() > 0) {
      ss << std::setw(2) << std::setfill('0');
    }
    ss << m.count() << 'm';
  }
  if (m.count() > 0) {
    ss << std::setw(2) << std::setfill('0');
  }
  ss << s.count() << '.'
     << std::setw(3) << std::setfill('0') << ms.count()
     << 's'
    ;
  return ss.str();
}

}  // namespace deephaven::dhcore::utility
