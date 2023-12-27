/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
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
void TestWheres(const TableHandleManager &scope);
void TestSelects(const TableHandleManager &scope);
void TestWheresHelper(std::string_view what, const TableHandle &table,
    const std::vector<std::string> &bad_wheres,
    const std::vector<std::string> &good_wheres);
void TestSelectsHelper(std::string_view what, const TableHandle &table,
    const std::vector<std::vector<std::string>> &bad_selects,
    const std::vector<std::vector<std::string>> &good_selects);
}  // namespace

TEST_CASE("Validate selects", "[validation]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();
  TestSelects(tm.Client().GetManager());
}

TEST_CASE("Validate wheres", "[validation]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();
  TestWheres(tm.Client().GetManager());
}

namespace {
void TestWheres(const TableHandleManager &scope) {
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

  auto static_table = scope.EmptyTable(10)
      .Update("X = 12", "S = `hello`");
  TestWheresHelper("static Table", static_table, bad_wheres, good_wheres);
}

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

void TestSelects(const TableHandleManager &scope) {
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
  auto static_table = scope.EmptyTable(10)
      .Update("X = 12", "S = `hello`");
  TestSelectsHelper("static Table", static_table, bad_selects, good_selects);
}

void TestSelectsHelper(std::string_view what, const TableHandle &table,
    const std::vector<std::vector<std::string>> &bad_selects,
    const std::vector<std::vector<std::string>> &good_selects) {
  for (const auto &bs : bad_selects) {
    SimpleOstringstream selection;
    selection << separatedList(bs.begin(), bs.end());
    try {
      (void)table.Select(bs);
    } catch (const std::exception &e) {
      fmt::print(std::cerr, "{}: {}: Failed as expected with: {}\n", what, selection.str(), e.what());
      continue;
    }
    throw std::runtime_error(fmt::format("{}: {}: Expected to fail, but succeeded",
        what, selection.str()));
  }

  for (const auto &gs : good_selects) {
    (void)table.Select(gs);
    fmt::print(std::cerr, "{}: {}: Succeeded as expected\n", what, gs);
  }
}
}  // namespace
}  // namespace deephaven::client::tests
