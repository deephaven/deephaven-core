/*
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/client/update_by.h"

using deephaven::client::update_by::BadDataBehavior;
using deephaven::client::update_by::cumMax;
using deephaven::client::update_by::cumMin;
using deephaven::client::update_by::cumProd;
using deephaven::client::update_by::cumSum;
using deephaven::client::update_by::delta;
using deephaven::client::update_by::DeltaControl;
using deephaven::client::update_by::emaTick;
using deephaven::client::update_by::emaTime;
using deephaven::client::update_by::emmaxTick;
using deephaven::client::update_by::emmaxTime;
using deephaven::client::update_by::emminTick;
using deephaven::client::update_by::emminTime;
using deephaven::client::update_by::emsTick;
using deephaven::client::update_by::emsTime;
using deephaven::client::update_by::emstdTick;
using deephaven::client::update_by::emstdTime;
using deephaven::client::update_by::forwardFill;
using deephaven::client::update_by::MathContext;
using deephaven::client::update_by::OperationControl;
using deephaven::client::update_by::rollingAvgTick;
using deephaven::client::update_by::rollingAvgTime;
using deephaven::client::update_by::rollingCountTick;
using deephaven::client::update_by::rollingCountTime;
using deephaven::client::update_by::rollingGroupTick;
using deephaven::client::update_by::rollingGroupTime;
using deephaven::client::update_by::rollingMaxTick;
using deephaven::client::update_by::rollingMaxTime;
using deephaven::client::update_by::rollingMinTick;
using deephaven::client::update_by::rollingMinTime;
using deephaven::client::update_by::rollingProdTick;
using deephaven::client::update_by::rollingProdTime;
using deephaven::client::update_by::rollingStdTick;
using deephaven::client::update_by::rollingStdTime;
using deephaven::client::update_by::rollingSumTime;
using deephaven::client::update_by::rollingSumTick;
using deephaven::client::update_by::rollingSumTime;
using deephaven::client::update_by::rollingWavgTick;
using deephaven::client::update_by::rollingWavgTime;
using deephaven::dhcore::utility::makeReservedVector;
using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {
namespace {
constexpr size_t numCols = 5;
constexpr size_t numRows = 1000;

std::vector<TableHandle> makeTables(const Client &client);
std::vector<UpdateByOperation> makeSimpleOps();
std::vector<UpdateByOperation> makeEmOps();
std::vector<UpdateByOperation> makeRollingOps();
TableHandle makeRandomTable(const Client &client);
}
TEST_CASE("UpdateBy: SimpleCumSum", "[update_by]") {
  auto client = TableMakerForTests::createClient();
  auto tm = client.getManager();
  auto source = tm.emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i");
  auto result = source.updateBy({cumSum({"SumX = X"})}, {"Letter"});
  auto filtered = result.select("SumX");
  compareTable(filtered, "SumX", std::vector<int64_t>{0, 1, 2, 4, 6, 9, 12, 16, 20, 25});
}

TEST_CASE("UpdateBy: SimpleOps", "[update_by]") {
  auto client = TableMakerForTests::createClient();
  auto tables = makeTables(client);
  auto simpleOps = makeSimpleOps();

  for (size_t opIndex = 0; opIndex != simpleOps.size(); ++opIndex) {
    const auto &op = simpleOps[opIndex];
    for (size_t tableIndex = 0; tableIndex != tables.size(); ++tableIndex) {
      const auto &table = tables[tableIndex];
      INFO("Processing op " << opIndex << " on table " << tableIndex);
      auto result = table.updateBy({op}, {"e"});
      CHECK(result.isStatic() == table.isStatic());
      CHECK(result.schema()->numCols() == 2 + table.schema()->numCols());
      CHECK(result.numRows() >= table.numRows());
    }
  }
}

TEST_CASE("UpdateBy: EmOps", "[update_by]") {
  auto client = TableMakerForTests::createClient();
  auto tables = makeTables(client);
  auto emOps = makeEmOps();

  for (size_t opIndex = 0; opIndex != emOps.size(); ++opIndex) {
    const auto &op = emOps[opIndex];
    for (size_t tableIndex = 0; tableIndex != tables.size(); ++tableIndex) {
      const auto &table = tables[tableIndex];
      INFO("Processing op " << opIndex << " on table " << tableIndex);
      auto result = table.updateBy({op}, {"b"});
      CHECK(result.isStatic() == table.isStatic());
      CHECK(result.schema()->numCols() == 1 + table.schema()->numCols());
      if (result.isStatic()) {
        CHECK(result.numRows() == table.numRows());
      }
    }
  }
}

TEST_CASE("UpdateBy: RollingOps", "[update_by]") {
  auto client = TableMakerForTests::createClient();
  auto tables = makeTables(client);
  auto rollingOps = makeRollingOps();

  for (size_t opIndex = 0; opIndex != rollingOps.size(); ++opIndex) {
    const auto &op = rollingOps[opIndex];
    for (size_t tableIndex = 0; tableIndex != tables.size(); ++tableIndex) {
      const auto &table = tables[tableIndex];
      INFO("Processing op " << opIndex << " on table " << tableIndex);
      auto result = table.updateBy({op}, {"c"});
      CHECK(result.isStatic() == table.isStatic());
      CHECK(result.schema()->numCols() == 2 + table.schema()->numCols());
      CHECK(result.numRows() >= table.numRows());
    }
  }
}

TEST_CASE("UpdateBy: Multiple Ops", "[update_by]") {
  auto client = TableMakerForTests::createClient();
  auto tables = makeTables(client);
  std::vector<UpdateByOperation> multipleOps = {
      cumSum({"sum_a=a", "sum_b=b"}),
      cumSum({"max_a=a", "max_d=d"}),
      emaTick(10, {"ema_d=d", "ema_e=e"}),
      emaTime("Timestamp", "PT00:00:00.1", {"ema_time_d=d", "ema_time_e=e"}),
      rollingWavgTick("b", {"rwavg_a = a", "rwavg_d = d"}, 10)
  };

  for (size_t tableIndex = 0; tableIndex != tables.size(); ++tableIndex) {
    const auto &table = tables[tableIndex];
    INFO("Processing table " << tableIndex);
    auto result = table.updateBy(multipleOps, {"c"});
    CHECK(result.isStatic() == table.isStatic());
    CHECK(result.schema()->numCols() == 10 + table.schema()->numCols());
    if (result.isStatic()) {
      CHECK(result.numRows() == table.numRows());
    }
  }
}

namespace {
std::vector<TableHandle> makeTables(const Client &client) {
  std::vector<TableHandle> result;
  auto tm = client.getManager();
  auto staticTable = makeRandomTable(client).update("Timestamp=now()");
  auto tickingTable = tm.timeTable(std::chrono::system_clock::now(), std::chrono::seconds(1))
      .update("a = i", "b = i*i % 13", "c = i * 13 % 23", "d = a + b", "e = a - b");
  return {staticTable, tickingTable};
}

TableHandle makeRandomTable(const Client &client) {
  std::random_device rd;
  std::default_random_engine engine(rd());
  std::uniform_int_distribution<int32_t> uniform_dist(0, 999);

  TableMaker tm;
  static_assert(numCols <= 26);
  for (size_t col = 0; col != numCols; ++col) {
    char name[2] = {char('a' + col), 0};
    auto values = makeReservedVector<int32_t>(numRows);
    for (size_t i = 0; i != numRows; ++i) {
      values.push_back(uniform_dist(engine));
    }
    tm.addColumn(name, values);
  }
  return tm.makeTable(client.getManager());
}

std::vector<UpdateByOperation> makeSimpleOps() {
  std::vector<std::string> simpleOpPairs = {"UA=a", "UB=b"};
  std::vector<UpdateByOperation> result = {
      cumSum(simpleOpPairs),
      cumProd(simpleOpPairs),
      cumMin(simpleOpPairs),
      cumMax(simpleOpPairs),
      forwardFill(simpleOpPairs),
      delta(simpleOpPairs),
      delta(simpleOpPairs, DeltaControl::NULL_DOMINATES),
      delta(simpleOpPairs, DeltaControl::VALUE_DOMINATES),
      delta(simpleOpPairs, DeltaControl::ZERO_DOMINATES)
  };
  return result;
}

std::vector<UpdateByOperation> makeEmOps() {
  OperationControl emOpControl(BadDataBehavior::THROW, BadDataBehavior::RESET,
      MathContext::UNLIMITED);

  using nanos = std::chrono::nanoseconds;

  std::vector<UpdateByOperation> result = {
      // exponential moving average
      emaTick(100, {"ema_a = a"}),
      emaTick(100, {"ema_a = a"}, emOpControl),
      emaTime("Timestamp", nanos(10), {"ema_a = a"}),
      emaTime("Timestamp", "PT00:00:00.001", {"ema_c = c"}, emOpControl),
      emaTime("Timestamp", "PT1M", {"ema_c = c"}),
      emaTime("Timestamp", "PT1M", {"ema_c = c"}, emOpControl),
      // exponential moving sum
      emsTick(100, {"ems_a = a"}),
      emsTick(100, {"ems_a = a"}, emOpControl),
      emsTime("Timestamp", nanos(10), {"ems_a = a"}),
      emsTime("Timestamp", "PT00:00:00.001", {"ems_c = c"}, emOpControl),
      emsTime("Timestamp", "PT1M", {"ema_c = c"}),
      emsTime("Timestamp", "PT1M", {"ema_c = c"}, emOpControl),
      // exponential moving minimum
      emminTick(100, {"emmin_a = a"}),
      emminTick(100, {"emmin_a = a"}, emOpControl),
      emminTime("Timestamp", nanos(10), {"emmin_a = a"}),
      emminTime("Timestamp", "PT00:00:00.001", {"emmin_c = c"}, emOpControl),
      emminTime("Timestamp", "PT1M", {"ema_c = c"}),
      emminTime("Timestamp", "PT1M", {"ema_c = c"}, emOpControl),
      // exponential moving maximum
      emmaxTick(100, {"emmax_a = a"}),
      emmaxTick(100, {"emmax_a = a"}, emOpControl),
      emmaxTime("Timestamp", nanos(10), {"emmax_a = a"}),
      emmaxTime("Timestamp", "PT00:00:00.001", {"emmax_c = c"}, emOpControl),
      emmaxTime("Timestamp", "PT1M", {"ema_c = c"}),
      emmaxTime("Timestamp", "PT1M", {"ema_c = c"}, emOpControl),
      // exponential moving standard deviation
      emstdTick(100, {"emstd_a = a"}),
      emstdTick(100, {"emstd_a = a"}, emOpControl),
      emstdTime("Timestamp", nanos(10), {"emstd_a = a"}),
      emstdTime("Timestamp", "PT00:00:00.001", {"emtd_c = c"}, emOpControl),
      emstdTime("Timestamp", "PT1M", {"ema_c = c"}),
      emstdTime("Timestamp", "PT1M", {"ema_c = c"}, emOpControl)
  };
  return result;
}

std::vector<UpdateByOperation> makeRollingOps() {
  OperationControl emOpControl(BadDataBehavior::THROW, BadDataBehavior::RESET,
      MathContext::UNLIMITED);

  using secs = std::chrono::seconds;

  // exponential moving average
  std::vector<UpdateByOperation> result = {
      // rolling sum
      rollingSumTick({"rsum_a = a", "rsum_d = d"}, 10),
      rollingSumTick({"rsum_a = a", "rsum_d = d"}, 10, 10),
      rollingSumTime("Timestamp", {"rsum_b = b", "rsum_e = e"}, "PT00:00:10"),
      rollingSumTime("Timestamp", {"rsum_b = b", "rsum_e = e"}, secs(10), secs(-10)),
      rollingSumTime("Timestamp", {"rsum_b = b", "rsum_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling group
      rollingGroupTick({"rgroup_a = a", "rgroup_d = d"}, 10),
      rollingGroupTick({"rgroup_a = a", "rgroup_d = d"}, 10, 10),
      rollingGroupTime("Timestamp", {"rgroup_b = b", "rgroup_e = e"}, "PT00:00:10"),
      rollingGroupTime("Timestamp", {"rgroup_b = b", "rgroup_e = e"}, secs(10), secs(-10)),
      rollingGroupTime("Timestamp", {"rgroup_b = b", "rgroup_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling average
      rollingAvgTick({"ravg_a = a", "ravg_d = d"}, 10),
      rollingAvgTick({"ravg_a = a", "ravg_d = d"}, 10, 10),
      rollingAvgTime("Timestamp", {"ravg_b = b", "ravg_e = e"}, "PT00:00:10"),
      rollingAvgTime("Timestamp", {"ravg_b = b", "ravg_e = e"}, secs(10), secs(-10)),
      rollingAvgTime("Timestamp", {"ravg_b = b", "ravg_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling minimum
      rollingMinTick({"rmin_a = a", "rmin_d = d"}, 10),
      rollingMinTick({"rmin_a = a", "rmin_d = d"}, 10, 10),
      rollingMinTime("Timestamp", {"rmin_b = b", "rmin_e = e"}, "PT00:00:10"),
      rollingMinTime("Timestamp", {"rmin_b = b", "rmin_e = e"}, secs(10), secs(-10)),
      rollingMinTime("Timestamp", {"rmin_b = b", "rmin_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling maximum
      rollingMaxTick({"rmax_a = a", "rmax_d = d"}, 10),
      rollingMaxTick({"rmax_a = a", "rmax_d = d"}, 10, 10),
      rollingMaxTime("Timestamp", {"rmax_b = b", "rmax_e = e"}, "PT00:00:10"),
      rollingMaxTime("Timestamp", {"rmax_b = b", "rmax_e = e"}, secs(10), secs(-10)),
      rollingMaxTime("Timestamp", {"rmax_b = b", "rmax_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling product
      rollingProdTick({"rprod_a = a", "rprod_d = d"}, 10),
      rollingProdTick({"rprod_a = a", "rprod_d = d"}, 10, 10),
      rollingProdTime("Timestamp", {"rprod_b = b", "rprod_e = e"}, "PT00:00:10"),
      rollingProdTime("Timestamp", {"rprod_b = b", "rprod_e = e"}, secs(10), secs(-10)),
      rollingProdTime("Timestamp", {"rprod_b = b", "rprod_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling count
      rollingCountTick({"rcount_a = a", "rcount_d = d"}, 10),
      rollingCountTick({"rcount_a = a", "rcount_d = d"}, 10, 10),
      rollingCountTime("Timestamp", {"rcount_b = b", "rcount_e = e"}, "PT00:00:10"),
      rollingCountTime("Timestamp", {"rcount_b = b", "rcount_e = e"}, secs(10), secs(-10)),
      rollingCountTime("Timestamp", {"rcount_b = b", "rcount_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling standard deviation
      rollingStdTick({"rstd_a = a", "rstd_d = d"}, 10),
      rollingStdTick({"rstd_a = a", "rstd_d = d"}, 10, 10),
      rollingStdTime("Timestamp", {"rstd_b = b", "rstd_e = e"}, "PT00:00:10"),
      rollingStdTime("Timestamp", {"rstd_b = b", "rstd_e = e"}, secs(10), secs(-10)),
      rollingStdTime("Timestamp", {"rstd_b = b", "rstd_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling weighted average (using "b" as the weight column)
      rollingWavgTick("b", {"rwavg_a = a", "rwavg_d = d"}, 10),
      rollingWavgTick("b", {"rwavg_a = a", "rwavg_d = d"}, 10, 10),
      rollingWavgTime("Timestamp", "b", {"rwavg_b = b", "rwavg_e = e"}, "PT00:00:10"),
      rollingWavgTime("Timestamp", "b", {"rwavg_b = b", "rwavg_e = e"}, secs(10), secs(-10)),
      rollingWavgTime("Timestamp", "b", {"rwavg_b = b", "rwavg_e = e"}, "PT30S", "-PT00:00:20")
  };
  return result;
}
}  // namespace
}  // namespace deephaven::client::tests
