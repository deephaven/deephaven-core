/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/format.h"
#include "deephaven/third_party/fmt/ostream.h"
#include "deephaven/third_party/fmt/ranges.h"

using deephaven::client::TableHandleManager;
using deephaven::client::TableHandle;
using deephaven::dhcore::utility::SimpleOstringstream;
using deephaven::dhcore::utility::separatedList;

namespace deephaven::client::tests {
namespace {
void TestWheresHelper(std::string_view what, const TableHandle &table,
    const std::vector<std::string> &bad_wheres,
    const std::vector<std::string> &good_wheres);
void TestSelectsHelper(std::string_view what, const TableHandle &table,
    const std::vector<std::vector<std::string>> &bad_selects,
    const std::vector<std::vector<std::string>> &good_selects);
}  // namespace

TEST_CASE("Validate selects", "[validation]") {
  std::vector<std::vector<std::string>> bad_selects = {
      { "X = 3)" },
      { "S = `hello`", "T = java.util.regex.Pattern.quote(S)" }, // Pattern.quote not on whitelist
      { "X = Math.min(3, 4)" } // Math.min not on whitelist
  };
  std::vector<std::vector<std::string>> good_selects = {
      {"X = 3"},
      {"S = `hello`", "T = S.length()"}, // instance methods of String ok
      {"X = min(3, 4)"}, // "builtin" from GroovyStaticImports
      {"X = isFinite(3)"}, // another builtin from GroovyStaticImports
  };

  auto tm = TableMakerForTests::Create();
  auto thm = tm.Client().GetManager();
  auto static_table = thm.EmptyTable(10)
      .Update("X = 12", "S = `hello`");
  TestSelectsHelper("static Table", static_table, bad_selects, good_selects);
}

TEST_CASE("Validate wheres", "[validation]") {
  std::vector<std::string> bad_wheres = {
      "X > 3)", // syntax error
      "S = new String(`hello`)", // new not allowed
      "S = java.util.regex.Pattern.quote(S)", // Pattern.quote not on whitelist
      "X = Math.min(3, 4)" // Math.min not on whitelist
  };

  std::vector<std::string> good_wheres = {
      "X = 3",
      "S = `hello`",
      "S.length() = 17", // instance methods of String ok
      "X = min(3, 4)", // "builtin" from GroovyStaticImports
      "X = isFinite(3)", // another builtin from GroovyStaticImports
      "X in 3, 4, 5",
  };

  auto tm = TableMakerForTests::Create();
  auto thm = tm.Client().GetManager();
  auto static_table = thm.EmptyTable(10)
      .Update("X = 12", "S = `hello`");
  TestWheresHelper("static Table", static_table, bad_wheres, good_wheres);
}

namespace {
void TestWheresHelper(std::string_view what, const TableHandle &table,
    const std::vector<std::string> &bad_wheres,
    const std::vector<std::string> &good_wheres) {
  for (const auto &bw : bad_wheres) {
    try {
      fmt::print(std::cerr, "Trying {} {}\n", what, bw);
      (void)table.Where(bw);
    } catch (const std::exception &e) {
      fmt::print(std::cerr, "{}: {}: Failed *as expected* with: {}\n", what, bw, e.what());
      continue;
    }

    throw std::runtime_error(fmt::format("{}: {}: Expected to fail, but succeeded", what, bw));
  }

  for (const auto &gw : good_wheres) {
    (void)table.Where(gw);
    fmt::print(std::cerr, "{}: {}: Succeeded as expected\n", what, gw);
  }
}

void TestSelectsHelper(std::string_view what, const TableHandle &table,
    const std::vector<std::vector<std::string>> &bad_selects,
    const std::vector<std::vector<std::string>> &good_selects) {
  for (const auto &bs : bad_selects) {
    try {
      (void)table.Select(bs);
    } catch (const std::exception &e) {
      fmt::print(std::cerr, "{}: {}: Failed as expected with: {}\n", what, bs, e.what());
      continue;
    }
    throw std::runtime_error(fmt::format("{}: {}: Expected to fail, but succeeded",
        what, bs));
  }

  for (const auto &gs : good_selects) {
    (void)table.Select(gs);
    fmt::print(std::cerr, "{}: {}: Succeeded as expected\n", what, gs);
  }
}
}  // namespace
}  // namespace deephaven::client::tests
