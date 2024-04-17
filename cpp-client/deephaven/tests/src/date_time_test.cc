/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/core.h"
#include "deephaven/third_party/fmt/format.h"

using deephaven::dhcore::DateTime;
using deephaven::dhcore::utility::Base64Encode;
using deephaven::dhcore::utility::ObjectId;

namespace deephaven::client::tests {
TEST_CASE("DateTime constructor", "[datetime]") {
  DateTime dt0(2001, 3, 1, 12, 34, 56);
  CHECK(dt0.Nanos() == 983450096000000000);
  DateTime dt1(2001, 3, 1, 12, 34, 56, 987000000);
  CHECK(dt1.Nanos() == 983450096987000000);
  DateTime dt2(2001, 3, 1, 12, 34, 56, 987654000);
  CHECK(dt2.Nanos() == 983450096987654000);
  DateTime dt3(2001, 3, 1, 12, 34, 56, 987654321);
  CHECK(dt3.Nanos() == 983450096987654321);
}

TEST_CASE("DateTime parse ISO8601", "[datetime]") {
  constexpr const uint64_t kOneBillion = 1'000'000'000;

  auto dt1 = DateTime::Parse("2001-03-01T12:34:56-0500");
  CHECK(dt1.Nanos() == 983468096 * kOneBillion);

  auto dt2 = DateTime::Parse("2001-03-01T12:34:56-0400");
  CHECK(dt2.Nanos() == 983464496 * kOneBillion);

  auto dt3 = DateTime::Parse("2001-03-01T12:34:56Z");
  CHECK(dt3.Nanos() == 983450096 * kOneBillion);

  auto dt4 = DateTime::Parse("2001-03-01T12:34:56.987-0500");
  CHECK(dt4.Nanos() == 983468096987000000);

  auto dt5 = DateTime::Parse("2001-03-01T12:34:56.987654-0500");
  CHECK(dt5.Nanos() == 983468096987654000);

  auto dt6 = DateTime::Parse("2001-03-01T12:34:56.987654321-0500");
  CHECK(dt6.Nanos() == 983468096987654321);
}

TEST_CASE("DateTime format ISO8601", "[datetime]") {
  constexpr const uint64_t kOneBillion = 1'000'000'000;

  DateTime dt1(983468096 * kOneBillion);
  CHECK(fmt::to_string(dt1) == "2001-03-01T17:34:56.000000000Z");

  DateTime dt2(983464496 * kOneBillion);
  CHECK(fmt::to_string(dt2) == "2001-03-01T16:34:56.000000000Z");

  DateTime dt3(983450096 * kOneBillion);
  CHECK(fmt::to_string(dt3) == "2001-03-01T12:34:56.000000000Z");

  DateTime dt4(983468096987000000);
  CHECK(fmt::to_string(dt4) == "2001-03-01T17:34:56.987000000Z");

  DateTime dt5(983468096987654000);
  CHECK(fmt::to_string(dt5) == "2001-03-01T17:34:56.987654000Z");

  DateTime dt6(983468096987654321);
  CHECK(fmt::to_string(dt6) == "2001-03-01T17:34:56.987654321Z");
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

TEST_CASE("DateTime parse fractions", "[datetime]") {
  auto dt0 = DateTime::Parse("2001-03-01T12:34:56.000-0500");
  CHECK(dt0.Nanos() == 983468096000000000);
  auto dt1 = DateTime::Parse("2001-03-01T12:34:56.987-0500");
  CHECK(dt1.Nanos() == 983468096987000000);
  auto dt2 = DateTime::Parse("2001-03-01T12:34:56.987654-0500");
  CHECK(dt2.Nanos() == 983468096987654000);
  auto dt3 = DateTime::Parse("2001-03-01T12:34:56.987654321-0500");
  CHECK(dt3.Nanos() == 983468096987654321);
}
}  // namespace deephaven::client::tests
