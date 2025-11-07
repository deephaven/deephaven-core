/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include <cstdint>
#include <iostream>
#include <memory>
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/catch.hpp"
#include "deephaven/third_party/fmt/core.h"

using deephaven::dhcore::utility::Base64Encode;
using deephaven::dhcore::utility::Basename;
using deephaven::dhcore::utility::EpochMillisToStr;
using deephaven::dhcore::utility::GetEnv;
using deephaven::dhcore::utility::GetTidAsString;
using deephaven::dhcore::utility::GetHostname;
using deephaven::dhcore::utility::ObjectId;
using deephaven::dhcore::utility::SetEnv;
using deephaven::dhcore::utility::UnsetEnv;
using deephaven::dhcore::utility::VerboseCast;
using deephaven::dhcore::utility::VerboseSharedPtrCast;

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
// that it returns the right value.
TEST_CASE("GetEnv", "[utility]") {
#if defined(__unix__)
  const char *expected_key = "PATH";
#elif defined(_WIN32)
  const char *expected_key = "OS";
#else
#error "Unsupported configuration"
#endif

  auto value = GetEnv(expected_key);
  REQUIRE(value.has_value());
  fmt::println("{} is: {}", expected_key, *value);
}

// Confirm that SetEnv leaves something that GetEnv can find.
TEST_CASE("SetEnv", "[utility]") {
  std::string unlikely_key = "Deephaven__serious_realtime_data_tools";
  std::string unlikely_value = "query_engine_APIs_and_user_interfaces";
  {
    auto value = GetEnv(unlikely_key);
    if (value.has_value()) {
      fmt::println(std::cerr, "unexpected value is {}", *value);
    }
    REQUIRE(!value.has_value());
  }

  SetEnv(unlikely_key, unlikely_value);
  {
    auto value = GetEnv(unlikely_key);
    REQUIRE(unlikely_value == value);
  }

  UnsetEnv(unlikely_key);
  {
    auto value = GetEnv(unlikely_key);
    REQUIRE(!value.has_value());
  }
}

namespace {
struct Mammal {
  virtual ~Mammal() = default;
};

struct Cow : Mammal {
};

struct Fish {
  virtual ~Fish() = default;
};
}  // namespace

TEST_CASE("VerboseCast", "[utility]") {
  auto cow = std::make_shared<Cow>();
  std::shared_ptr<Mammal> mammal = cow;

  CHECK_NOTHROW(VerboseCast<Cow*>(DEEPHAVEN_LOCATION_EXPR(mammal.get())));
  CHECK_THROWS(VerboseCast<Fish*>(DEEPHAVEN_LOCATION_EXPR(mammal.get())));

  CHECK_NOTHROW(VerboseSharedPtrCast<Cow>(DEEPHAVEN_LOCATION_EXPR(mammal)));
  CHECK_THROWS(VerboseSharedPtrCast<Fish>(DEEPHAVEN_LOCATION_EXPR(mammal)));
}
}  // namespace deephaven::client::tests
