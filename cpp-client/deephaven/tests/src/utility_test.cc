/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::utility::Base64Encode;
using deephaven::dhcore::utility::Basename;
using deephaven::dhcore::utility::EpochMillisToStr;
using deephaven::dhcore::utility::GetEnv;
using deephaven::dhcore::utility::GetTidAsString;
using deephaven::dhcore::utility::GetHostname;
using deephaven::dhcore::utility::ObjectId;

namespace deephaven::client::tests {
TEST_CASE("Base64encode", "[utility]") {
  // https://en.wikipedia.org/wiki/Base64
  CHECK(Base64Encode("light work.") == "bGlnaHQgd29yay4=");
  CHECK(Base64Encode("light work") == "bGlnaHQgd29yaw==");
  CHECK(Base64Encode("light wor") == "bGlnaHQgd29y");
}

TEST_CASE("EpochMillisToStr", "[utility]") {
  auto d1 = EpochMillisToStr(1689136824000);
  auto d2 = EpochMillisToStr(123456);
  CHECK(d1 == "2023-07-12T04:40:24.000Z");
  CHECK(d2 == "1970-01-01T00:02:03.456Z");
}

TEST_CASE("ObjectId", "[utility]") {
  uintptr_t p = 0xdeadbeef;
  auto id = ObjectId("hello", reinterpret_cast<void *>(p));
  CHECK(id == "hello(0xdeadbeef)");
}

TEST_CASE("Basename", "[utility]") {
#ifdef __linux__
  CHECK("file.txt" == Basename("/home/kosak/file.txt"));
  CHECK(Basename("/home/kosak/").empty());
#endif

#ifdef _WIN32
  CHECK("file.txt" == Basename(R"(C:\Users\kosak\file.txt)"));
  CHECK(Basename(R"(C:\Users\kosak\)").empty());
#endif
}

// This isn't much of a test, but if it can compile on all supported
// platforms (Linux and Windows) then that is at least a sanity check
// (that the entry point exists). For now we just visually spot-check
// that ireturns the right value.
TEST_CASE("ThreadId", "[utility]") {
  auto tid = GetTidAsString();
  fmt::println("This should be my thread id: {}", tid);
}

// This isn't much of a test, but if it can compile on all supported
// platforms (Linux and Windows) then that is at least a sanity check
// (that the entry point exists). For now we just visually spot-check
// that ireturns the right value.
TEST_CASE("GetHostname", "[utility]") {
  auto hostname = GetHostname();
  fmt::println("This should be the hostname: {}", hostname);
}

// This isn't much of a test, but if it can compile on all supported
// platforms (Linux and Windows) then that is at least a sanity check
// (that the entry point exists). For now we just visually spot-check
// that ireturns the right value.
TEST_CASE("GetEnv", "[utility]") {
  auto path = GetEnv("PATH");
  // Very suspect if neither Windows nor Linux has a PATH set in their
  // environment.
  REQUIRE(path.has_value());
  fmt::println("PATH is: {}", *path);
}
}  // namespace deephaven::client::tests
