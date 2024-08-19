/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
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
using deephaven::dhcore::utility::MakeReservedVector;
using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {
namespace {
constexpr size_t kNumCols = 5;
constexpr size_t kNumRows = 1000;

[[nodiscard]]
std::vector<TableHandle> MakeTables(const Client &client);
[[nodiscard]]
std::vector<UpdateByOperation> MakeSimpleOps();
[[nodiscard]]
std::vector<UpdateByOperation> MakeEmOps();
[[nodiscard]]
std::vector<UpdateByOperation> MakeRollingOps();
[[nodiscard]]
TableHandle MakeRandomTable(const Client &client);
}
TEST_CASE("UpdateBy: SimpleCumSum", "[update_by]") {
  auto client = TableMakerForTests::CreateClient();
  auto tm = client.GetManager();
  auto source = tm.EmptyTable(10).Update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i");
  auto result = source.UpdateBy({cumSum({"SumX = X"})}, {"Letter"});
  auto filtered = result.Select("SumX");
  CompareTable(filtered, "SumX", std::vector<int64_t>{0, 1, 2, 4, 6, 9, 12, 16, 20, 25});
}

TEST_CASE("UpdateBy: SimpleOps", "[update_by]") {
  auto client = TableMakerForTests::CreateClient();
  auto tables = MakeTables(client);
  auto simple_ops = MakeSimpleOps();

  for (size_t op_index = 0; op_index != simple_ops.size(); ++op_index) {
    const auto &op = simple_ops[op_index];
    for (size_t table_index = 0; table_index != tables.size(); ++table_index) {
      const auto &table = tables[table_index];
      INFO("Processing op " << op_index << " on Table " << table_index);
      auto result = table.UpdateBy({op}, {"e"});
      CHECK(result.IsStatic() == table.IsStatic());
      CHECK(result.Schema()->NumCols() == 2 + table.Schema()->NumCols());
      CHECK(result.NumRows() >= table.NumRows());
    }
  }
}

TEST_CASE("UpdateBy: EmOps", "[update_by]") {
  auto client = TableMakerForTests::CreateClient();
  auto tables = MakeTables(client);
  auto em_ops = MakeEmOps();

  for (size_t op_index = 0; op_index != em_ops.size(); ++op_index) {
    const auto &op = em_ops[op_index];
    for (size_t table_index = 0; table_index != tables.size(); ++table_index) {
      const auto &table = tables[table_index];
      INFO("Processing op " << op_index << " on Table " << table_index);
      auto result = table.UpdateBy({op}, {"b"});
      CHECK(result.IsStatic() == table.IsStatic());
      CHECK(result.Schema()->NumCols() == 1 + table.Schema()->NumCols());
      if (result.IsStatic()) {
        CHECK(result.NumRows() == table.NumRows());
      }
    }
  }
}

TEST_CASE("UpdateBy: RollingOps", "[update_by]") {
  auto client = TableMakerForTests::CreateClient();
  auto tables = MakeTables(client);
  auto rolling_ops = MakeRollingOps();

  for (size_t op_index = 0; op_index != rolling_ops.size(); ++op_index) {
    const auto &op = rolling_ops[op_index];
    for (size_t table_index = 0; table_index != tables.size(); ++table_index) {
      const auto &table = tables[table_index];
      INFO("Processing op " << op_index << " on Table " << table_index);
      auto result = table.UpdateBy({op}, {"c"});
      CHECK(result.IsStatic() == table.IsStatic());
      CHECK(result.Schema()->NumCols() == 2 + table.Schema()->NumCols());
      CHECK(result.NumRows() >= table.NumRows());
    }
  }
}

TEST_CASE("UpdateBy: Multiple Ops", "[update_by]") {
  auto client = TableMakerForTests::CreateClient();
  auto tables = MakeTables(client);
  std::vector<UpdateByOperation> multiple_ops = {
      cumSum({"sum_a=a", "sum_b=b"}),
      cumSum({"max_a=a", "max_d=d"}),
      emaTick(10, {"ema_d=d", "ema_e=e"}),
      emaTime("Timestamp", "PT00:00:00.1", {"ema_time_d=d", "ema_time_e=e"}),
      rollingWavgTick("b", {"rwavg_a = a", "rwavg_d = d"}, 10)
  };

  for (size_t table_index = 0; table_index != tables.size(); ++table_index) {
    const auto &table = tables[table_index];
    INFO("Processing Table " << table_index);
    auto result = table.UpdateBy(multiple_ops, {"c"});
    CHECK(result.IsStatic() == table.IsStatic());
    CHECK(result.Schema()->NumCols() == 10 + table.Schema()->NumCols());
    if (result.IsStatic()) {
      CHECK(result.NumRows() == table.NumRows());
    }
  }
}

namespace {
std::vector<TableHandle> MakeTables(const Client &client) {
  auto tm = client.GetManager();
  auto static_table = MakeRandomTable(client).Update("Timestamp=now()");
  auto ticking_table = tm.TimeTable(std::chrono::seconds(1))
      .Update("a = i", "b = i*i % 13", "c = i * 13 % 23", "d = a + b", "e = a - b");
  return {static_table, ticking_table};
}

TableHandle MakeRandomTable(const Client &client) {
  std::random_device rd;
  std::default_random_engine engine(rd());
  std::uniform_int_distribution<int32_t> uniform_dist(0, 999);

  TableMaker tm;
  static_assert(kNumCols <= 26);
  for (size_t col = 0; col != kNumCols; ++col) {
    char name[2] = {static_cast<char>('a' + col), 0};
    auto values = MakeReservedVector<int32_t>(kNumRows);
    for (size_t i = 0; i != kNumRows; ++i) {
      values.push_back(uniform_dist(engine));
    }
    tm.AddColumn(name, values);
  }
  return tm.MakeTable(client.GetManager());
}

std::vector<UpdateByOperation> MakeSimpleOps() {
  std::vector<std::string> simple_op_pairs = {"UA=a", "UB=b"};
  std::vector<UpdateByOperation> result = {
      cumSum(simple_op_pairs),
      cumProd(simple_op_pairs),
      cumMin(simple_op_pairs),
      cumMax(simple_op_pairs),
      forwardFill(simple_op_pairs),
      delta(simple_op_pairs),
      delta(simple_op_pairs, DeltaControl::kNullDominates),
      delta(simple_op_pairs, DeltaControl::kValueDominates),
      delta(simple_op_pairs, DeltaControl::kZeroDominates)
  };
  return result;
}

std::vector<UpdateByOperation> MakeEmOps() {
  OperationControl em_op_control(BadDataBehavior::kThrow, BadDataBehavior::kReset,
      MathContext::kUnlimited);

  using nanos = std::chrono::nanoseconds;

  std::vector<UpdateByOperation> result = {
      // exponential moving average
      emaTick(100, {"ema_a = a"}),
      emaTick(100, {"ema_a = a"}, em_op_control),
      emaTime("Timestamp", nanos(10), {"ema_a = a"}),
      emaTime("Timestamp", "PT00:00:00.001", {"ema_c = c"}, em_op_control),
      emaTime("Timestamp", "PT1M", {"ema_c = c"}),
      emaTime("Timestamp", "PT1M", {"ema_c = c"}, em_op_control),
      // exponential moving sum
      emsTick(100, {"ems_a = a"}),
      emsTick(100, {"ems_a = a"}, em_op_control),
      emsTime("Timestamp", nanos(10), {"ems_a = a"}),
      emsTime("Timestamp", "PT00:00:00.001", {"ems_c = c"}, em_op_control),
      emsTime("Timestamp", "PT1M", {"ema_c = c"}),
      emsTime("Timestamp", "PT1M", {"ema_c = c"}, em_op_control),
      // exponential moving minimum
      emminTick(100, {"emmin_a = a"}),
      emminTick(100, {"emmin_a = a"}, em_op_control),
      emminTime("Timestamp", nanos(10), {"emmin_a = a"}),
      emminTime("Timestamp", "PT00:00:00.001", {"emmin_c = c"}, em_op_control),
      emminTime("Timestamp", "PT1M", {"ema_c = c"}),
      emminTime("Timestamp", "PT1M", {"ema_c = c"}, em_op_control),
      // exponential moving maximum
      emmaxTick(100, {"emmax_a = a"}),
      emmaxTick(100, {"emmax_a = a"}, em_op_control),
      emmaxTime("Timestamp", nanos(10), {"emmax_a = a"}),
      emmaxTime("Timestamp", "PT00:00:00.001", {"emmax_c = c"}, em_op_control),
      emmaxTime("Timestamp", "PT1M", {"ema_c = c"}),
      emmaxTime("Timestamp", "PT1M", {"ema_c = c"}, em_op_control),
      // exponential moving standard deviation
      emstdTick(100, {"emstd_a = a"}),
      emstdTick(100, {"emstd_a = a"}, em_op_control),
      emstdTime("Timestamp", nanos(10), {"emstd_a = a"}),
      emstdTime("Timestamp", "PT00:00:00.001", {"emtd_c = c"}, em_op_control),
      emstdTime("Timestamp", "PT1M", {"ema_c = c"}),
      emstdTime("Timestamp", "PT1M", {"ema_c = c"}, em_op_control)
  };
  return result;
}

std::vector<UpdateByOperation> MakeRollingOps() {
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
