/*
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::DateTime;
using deephaven::dhcore::utility::Base64Encode;
using deephaven::dhcore::utility::EpochMillisToStr;
using deephaven::dhcore::utility::ObjectId;

namespace deephaven::client::tests {
TEST_CASE("DateTime parse ISO8601", "[datetime]") {
  constexpr const uint64_t kOneBillion = 1'000'000'000;

  auto dt1 = DateTime::Parse("2001-03-01T12:34:56-0500");
  CHECK(dt1.Nanos() == 983468096 * kOneBillion);

  auto dt2 = DateTime::Parse("2001-03-01T12:34:56-0400");
  CHECK(dt2.Nanos() == 983464496 * kOneBillion);
}

TEST_CASE("DateTime parse fails", "[datetime]") {
  std::string correct_string = "2001-03-01T12:34:56-0500";
  // Add some junk to the end.
  std::string complete = correct_string + "abc123";
  for (size_t i = 0; i <= complete.size(); ++i) {
    auto sub = complete.substr(0, i);
    INFO("length " << i << ": " << sub);
    if (i == correct_string.size() - 2 || i == correct_string.size()) {
      // parse ok for the timezone -05 or -0500
      CHECK_NOTHROW(DateTime::Parse(sub));
    } else {
      // parse fail for every other substring
      CHECK_THROWS(DateTime::Parse(sub));
    }
  }
}

//TODO(kosak): implement fractional seconds, then remove the !mayfail tag
TEST_CASE("DateTime parse fractions", "[datetime][!mayfail]") {

  auto dt1 = DateTime::Parse("2001-03-01T12:34:56.987-0500");
  CHECK(dt1.Nanos() == 1234567);
  auto dt2 = DateTime::Parse("2001-03-01T12:34:56.987654-0500");
  CHECK(dt2.Nanos() == 1234567);
  auto dt3 = DateTime::Parse("2001-03-01T12:34:56.987654321-0500");
  CHECK(dt3.Nanos() == 1234567);
}

//TODO(kosak): implement standard timezones, then remove the !mayfail tag
TEST_CASE("DateTime parse Timezones", "[datetime][!mayfail]") {
  auto dt1 = DateTime::Parse("2001-03-01T12:34:56.987 EST");
  CHECK(dt1.Nanos() == 1234567);
  auto dt2 = DateTime::Parse("2001-03-01T12:34:56.987654 PST");
  CHECK(dt2.Nanos() == 1234567);
}

//TODO(kosak): implement Deephaven timezones, then remove the !mayfail tag
TEST_CASE("DateTime parse Deephaven Timezones", "[datetime][!mayfail]") {
  auto dt1 = DateTime::Parse("2001-03-01T12:34:56.987 NY");
  CHECK(dt1.Nanos() == 1234567);
  auto dt2 = DateTime::Parse("2001-03-01T12:34:56.987654 ET");
  CHECK(dt2.Nanos() == 1234567);
}
}  // namespace deephaven::client::tests
