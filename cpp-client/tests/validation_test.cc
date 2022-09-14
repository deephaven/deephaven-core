/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::TableHandleManager;
using deephaven::client::TableHandle;
using deephaven::client::utility::SimpleOstringstream;
using deephaven::client::utility::separatedList;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;

namespace deephaven::client::tests {
namespace {
void testWheres(const TableHandleManager &scope);
void testSelects(const TableHandleManager &scope);
void testWheresHelper(std::string_view what, const TableHandle &table,
    const std::vector<std::string> &badWheres,
    const std::vector<std::string> &goodWheres);
void testSelectsHelper(std::string_view what, const TableHandle &table,
    const std::vector<std::vector<std::string>> &badSelects,
    const std::vector<std::vector<std::string>> &goodSelects);
}  // namespace

TEST_CASE("Validate selects", "[validation]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();
  testSelects(tm.client().getManager());
}

TEST_CASE("Validate wheres", "[validation]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();
  testWheres(tm.client().getManager());
}

namespace {
void testWheres(const TableHandleManager &scope) {
  std::vector<std::string> badWheres = {
      "X > 3)", // syntax error
      "S = new String(`hello`)", // new not allowed
      "S = java.util.regex.Pattern.quote(S)", // Pattern.quote not on whitelist
      "X = Math.min(3, 4)" // Math.min not on whitelist
  };

  std::vector<std::string> goodWheres = {
      "X = 3",
      "S = `hello`",
      "S.length() = 17", // instance methods of String ok
      "X = min(3, 4)", // "builtin" from GroovyStaticImports
      "X = isFinite(3)", // another builtin from GroovyStaticImports
      "X in 3, 4, 5",
  };

  auto staticTable = scope.emptyTable(10)
      .update("X = 12", "S = `hello`");
  testWheresHelper("static table", staticTable, badWheres, goodWheres);
}

void testWheresHelper(std::string_view what, const TableHandle &table,
    const std::vector<std::string> &badWheres,
    const std::vector<std::string> &goodWheres) {
  for (const auto &bw : badWheres) {
    try {
      streamf(std::cerr, "Trying %o %o\n", what, bw);
      auto t1 = table.where(bw);
      t1.observe();
    } catch (const std::exception &e) {
      streamf(std::cerr, "%o: %o: Failed *as expected* with: %o\n", what, bw, e.what());
      continue;
    }

    throw std::runtime_error(stringf("%o: %o: Expected to fail, but succeeded", what, bw));
  }

  for (const auto &gw : goodWheres) {
    table.where(gw).observe();
    streamf(std::cerr, "%o: %o: Succeeded as expected\n", what, gw);
  }
}

void testSelects(const TableHandleManager &scope) {
  std::vector<std::vector<std::string>> badSelects = {
      { "X = 3)" },
      { "S = `hello`", "T = java.util.regex.Pattern.quote(S)" }, // Pattern.quote not on whitelist
      { "X = Math.min(3, 4)" } // Math.min not on whitelist
  };
  std::vector<std::vector<std::string>> goodSelects = {
      {"X = 3"},
      {"S = `hello`", "T = S.length()"}, // instance methods of String ok
      {"X = min(3, 4)"}, // "builtin" from GroovyStaticImports
      {"X = isFinite(3)"}, // another builtin from GroovyStaticImports
  };
  auto staticTable = scope.emptyTable(10)
      .update("X = 12", "S = `hello`");
  testSelectsHelper("static table", staticTable, badSelects, goodSelects);
}

void testSelectsHelper(std::string_view what, const TableHandle &table,
    const std::vector<std::vector<std::string>> &badSelects,
    const std::vector<std::vector<std::string>> &goodSelects) {
  for (const auto &bs : badSelects) {
    SimpleOstringstream selection;
    selection << separatedList(bs.begin(), bs.end());
    try {
      table.select(bs).observe();
    } catch (const std::exception &e) {
      streamf(std::cerr, "%o: %o: Failed as expected with: %o\n", what, selection.str(), e.what());
      continue;
    }
    throw std::runtime_error(stringf("%o: %o: Expected to fail, but succeeded",
        what, selection.str()));
  }

  for (const auto &gs : goodSelects) {
    table.select(gs).observe();
    streamf(std::cerr, "%o: %o: Succeeded as expected\n", what,
        separatedList(gs.begin(), gs.end()));
  }
}
}  // namespace
}  // namespace deephaven::client::tests
