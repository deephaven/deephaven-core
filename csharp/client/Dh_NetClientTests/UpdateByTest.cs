//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;
using Xunit.Abstractions;
using static Deephaven.Dh_NetClient.UpdateByOperation;

namespace Deephaven.Dh_NetClientTests;

public class UpdateByTest(ITestOutputHelper output) {
  const int NumCols = 5;
  const int NumRows = 1000;

  [Fact]
  public void SimpleCumSum() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;

    var source = tm.EmptyTable(10).Update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i");
    var result = source.UpdateBy([CumSum("SumX = X")], "Letter");
    var filtered = result.Select("SumX");

    var expected = new TableMaker();
    expected.AddColumn("SumX", new Int64[] { 0, 1, 2, 4, 6, 9, 12, 16, 20, 25 });
    TableComparer.AssertSame(expected, filtered);
  }

  [Fact]
  public void SimpleOps() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;

    var tables = MakeTables(tm);
    var simpleOps = MakeSimpleOps();

    for (var opIndex = 0; opIndex != simpleOps.Length; ++opIndex) {
      var op = simpleOps[opIndex];
      for (var tableIndex = 0; tableIndex != tables.Length; ++tableIndex) {
        var table = tables[tableIndex];
        output.WriteLine($"Processing op {opIndex} on Table {tableIndex}");
        using var result = table.UpdateBy([op], "e");
        Assert.Equal(table.IsStatic, result.IsStatic);
        Assert.Equal(2 + table.NumCols, result.NumCols);
        Assert.True(result.NumRows >= table.NumRows);
      }
    }
  }

  [Fact]
  public void EmOps() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;

    var tables = MakeTables(tm);
    var emOps = MakeEmOps();

    for (var opIndex = 0; opIndex != emOps.Length; ++opIndex) {
      var op = emOps[opIndex];
      for (var tableIndex = 0; tableIndex != tables.Length; ++tableIndex) {
        var table = tables[tableIndex];
        output.WriteLine($"Processing op {opIndex} on Table {tableIndex}");
        using var result = table.UpdateBy([op], "b");
        Assert.Equal(table.IsStatic, result.IsStatic);
        Assert.Equal(1 + table.NumCols, result.NumCols);
        if (result.IsStatic) {
          Assert.Equal(result.NumRows, table.NumRows);
        }
      }
    }
  }

  [Fact]
  public void RollingOps() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;

    var tables = MakeTables(tm);
    var rollingOps = MakeRollingOps();

    for (var opIndex = 0; opIndex != rollingOps.Length; ++opIndex) {
      var op = rollingOps[opIndex];
      for (var tableIndex = 0; tableIndex != tables.Length; ++tableIndex) {
        var table = tables[tableIndex];
        output.WriteLine($"Processing op {opIndex} on Table {tableIndex}");
        using var result = table.UpdateBy([op], "c");
        Assert.Equal(table.IsStatic, result.IsStatic);
        Assert.Equal(2 + table.NumCols, result.NumCols);
        Assert.True(result.NumRows >= table.NumRows);
      }
    }
  }

  [Fact]
  public void MultipleOps() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;

    var tables = MakeTables(tm);
    var multipleOps = new[] {
      CumSum("sum_a=a", "sum_b=b"),
      CumSum("max_a=a", "max_d=d"),
      EmaTick(10, ["ema_d=d", "ema_e=e"]),
      EmaTime("Timestamp", "PT00:00:00.1", ["ema_time_d=d", "ema_time_e=e"]),
      RollingWavgTick("b", ["rwavg_a = a", "rwavg_d = d"], 10)
    };

    for (var tableIndex = 0; tableIndex != tables.Length; ++tableIndex) {
      var table = tables[tableIndex];
      output.WriteLine($"Processing table {tableIndex}");
      using var result = table.UpdateBy(multipleOps, "c");
      Assert.Equal(table.IsStatic, result.IsStatic);
      Assert.Equal(10 + table.NumCols, result.NumCols);
      if (result.IsStatic) {
        Assert.Equal(result.NumRows, table.NumRows);
      }
    }
  }

  private static TableHandle[] MakeTables(TableHandleManager tm) {
    var staticTable = MakeRandomTable(tm).Update("Timestamp=now()");
    var tickingTable = tm.TimeTable(TimeSpan.FromSeconds(1))
      .Update("a = i", "b = i*i % 13", "c = i * 13 % 23", "d = a + b", "e = a - b");
    return [staticTable, tickingTable];
  }

  private static TableHandle MakeRandomTable(TableHandleManager tm) {
    var rng = new Random(12345);

    var maker = new TableMaker();
    if (NumCols > 26) {
      throw new ArgumentException("NumCols constant is too big for this test");
    }

    for (var col = 0; col != NumCols; ++col) {
      var name = ((char)('a' + col)).ToString();
      var values = new Int32[NumRows];
      for (var i = 0; i != NumRows; ++i) {
        values[i] = rng.Next(1000);
      }

      maker.AddColumn(name, values);
    }

    return maker.MakeTable(tm);
  }

  private static UpdateByOperation[] MakeSimpleOps() {
    var simpleOpPairs = new[] { "UA=a", "UB=b" };
    var result = new[] {
      CumSum(simpleOpPairs),
      CumProd(simpleOpPairs),
      CumMin(simpleOpPairs),
      CumMax(simpleOpPairs),
      ForwardFill(simpleOpPairs),
      Delta(simpleOpPairs),
      Delta(simpleOpPairs, DeltaControl.NullDominates),
      Delta(simpleOpPairs, DeltaControl.ValueDominates),
      Delta(simpleOpPairs, DeltaControl.ZeroDominates)
    };
    return result;
  }

  private static UpdateByOperation[] MakeEmOps() {
    var emOpControl = new OperationControl(BadDataBehavior.Throw, BadDataBehavior.Reset,
      MathContext.Unlimited);

    var result = new [] {
      // exponential moving average
      EmaTick(100, ["ema_a = a"]),
      EmaTick(100, ["ema_a = a"], emOpControl),
      EmaTime("Timestamp", 10, ["ema_a = a"]),
      EmaTime("Timestamp", "PT00:00:00.001", ["ema_c = c"], emOpControl),
      EmaTime("Timestamp", "PT1M", ["ema_c = c"]),
      EmaTime("Timestamp", "PT1M", ["ema_c = c"], emOpControl),
      // exponential moving sum
      EmsTick(100, ["ems_a = a"]),
      EmsTick(100, ["ems_a = a"], emOpControl),
      EmsTime("Timestamp", 10, ["ems_a = a"]),
      EmsTime("Timestamp", "PT00:00:00.001", ["ems_c = c"], emOpControl),
      EmsTime("Timestamp", "PT1M", ["ema_c = c"]),
      EmsTime("Timestamp", "PT1M", ["ema_c = c"], emOpControl),
      // exponential moving minimum
      EmMinTick(100, ["emmin_a = a"]),
      EmMinTick(100, ["emmin_a = a"], emOpControl),
      EmMinTime("Timestamp", 10, ["emmin_a = a"]),
      EmMinTime("Timestamp", "PT00:00:00.001", ["emmin_c = c"], emOpControl),
      EmMinTime("Timestamp", "PT1M", ["ema_c = c"]),
      EmMinTime("Timestamp", "PT1M", ["ema_c = c"], emOpControl),
      // exponential moving maximum
      EmMaxTick(100, ["emmax_a = a"]),
      EmMaxTick(100, ["emmax_a = a"], emOpControl),
      EmMaxTime("Timestamp", 10, ["emmax_a = a"]),
      EmMaxTime("Timestamp", "PT00:00:00.001", ["emmax_c = c"], emOpControl),
      EmMaxTime("Timestamp", "PT1M", ["ema_c = c"]),
      EmMaxTime("Timestamp", "PT1M", ["ema_c = c"], emOpControl),
      // exponential moving standard deviation
      EmStdTick(100, ["emstd_a = a"]),
      EmStdTick(100, ["emstd_a = a"], emOpControl),
      EmStdTime("Timestamp", 10, ["emstd_a = a"]),
      EmStdTime("Timestamp", "PT00:00:00.001", ["emtd_c = c"], emOpControl),
      EmStdTime("Timestamp", "PT1M", ["ema_c = c"]),
      EmStdTime("Timestamp", "PT1M", ["ema_c = c"], emOpControl)
    };

    return result;
  }

  private static UpdateByOperation[] MakeRollingOps() {
    static TimeSpan Secs(int s) => TimeSpan.FromSeconds(s);

    // exponential moving average
    var result = new[] {
      // rolling sum
      RollingSumTick(["rsum_a = a", "rsum_d = d"], 10),
      RollingSumTick(["rsum_a = a", "rsum_d = d"], 10, 10),
      RollingSumTime("Timestamp", ["rsum_b = b", "rsum_e = e"], "PT00:00:10"),
      RollingSumTime("Timestamp", ["rsum_b = b", "rsum_e = e"], Secs(10), Secs(-10)),
      RollingSumTime("Timestamp", ["rsum_b = b", "rsum_e = e"], "PT30S", "-PT00:00:20"),
      // rolling group
      RollingGroupTick(["rgroup_a = a", "rgroup_d = d"], 10),
      RollingGroupTick(["rgroup_a = a", "rgroup_d = d"], 10, 10),
      RollingGroupTime("Timestamp", ["rgroup_b = b", "rgroup_e = e"], "PT00:00:10"),
      RollingGroupTime("Timestamp", ["rgroup_b = b", "rgroup_e = e"], Secs(10), Secs(-10)),
      RollingGroupTime("Timestamp", ["rgroup_b = b", "rgroup_e = e"], "PT30S", "-PT00:00:20"),
      // rolling average
      RollingAvgTick(["ravg_a = a", "ravg_d = d"], 10),
      RollingAvgTick(["ravg_a = a", "ravg_d = d"], 10, 10),
      RollingAvgTime("Timestamp", ["ravg_b = b", "ravg_e = e"], "PT00:00:10"),
      RollingAvgTime("Timestamp", ["ravg_b = b", "ravg_e = e"], Secs(10), Secs(-10)),
      RollingAvgTime("Timestamp", ["ravg_b = b", "ravg_e = e"], "PT30S", "-PT00:00:20"),
      // rolling minimum
      RollingMinTick(["rmin_a = a", "rmin_d = d"], 10),
      RollingMinTick(["rmin_a = a", "rmin_d = d"], 10, 10),
      RollingMinTime("Timestamp", ["rmin_b = b", "rmin_e = e"], "PT00:00:10"),
      RollingMinTime("Timestamp", ["rmin_b = b", "rmin_e = e"], Secs(10), Secs(-10)),
      RollingMinTime("Timestamp", ["rmin_b = b", "rmin_e = e"], "PT30S", "-PT00:00:20"),
      // rolling maximum
      RollingMaxTick(["rmax_a = a", "rmax_d = d"], 10),
      RollingMaxTick(["rmax_a = a", "rmax_d = d"], 10, 10),
      RollingMaxTime("Timestamp", ["rmax_b = b", "rmax_e = e"], "PT00:00:10"),
      RollingMaxTime("Timestamp", ["rmax_b = b", "rmax_e = e"], Secs(10), Secs(-10)),
      RollingMaxTime("Timestamp", ["rmax_b = b", "rmax_e = e"], "PT30S", "-PT00:00:20"),
      // rolling product
      RollingProdTick(["rprod_a = a", "rprod_d = d"], 10),
      RollingProdTick(["rprod_a = a", "rprod_d = d"], 10, 10),
      RollingProdTime("Timestamp", ["rprod_b = b", "rprod_e = e"], "PT00:00:10"),
      RollingProdTime("Timestamp", ["rprod_b = b", "rprod_e = e"], Secs(10), Secs(-10)),
      RollingProdTime("Timestamp", ["rprod_b = b", "rprod_e = e"], "PT30S", "-PT00:00:20"),
      // rolling count
      RollingCountTick(["rcount_a = a", "rcount_d = d"], 10),
      RollingCountTick(["rcount_a = a", "rcount_d = d"], 10, 10),
      RollingCountTime("Timestamp", ["rcount_b = b", "rcount_e = e"], "PT00:00:10"),
      RollingCountTime("Timestamp", ["rcount_b = b", "rcount_e = e"], Secs(10), Secs(-10)),
      RollingCountTime("Timestamp", ["rcount_b = b", "rcount_e = e"], "PT30S", "-PT00:00:20"),
      // rolling standard deviation
      RollingStdTick(["rstd_a = a", "rstd_d = d"], 10),
      RollingStdTick(["rstd_a = a", "rstd_d = d"], 10, 10),
      RollingStdTime("Timestamp", ["rstd_b = b", "rstd_e = e"], "PT00:00:10"),
      RollingStdTime("Timestamp", ["rstd_b = b", "rstd_e = e"], Secs(10), Secs(-10)),
      RollingStdTime("Timestamp", ["rstd_b = b", "rstd_e = e"], "PT30S", "-PT00:00:20"),
      // rolling weighted average (using "b" as the weight column)
      RollingWavgTick("b", ["rwavg_a = a", "rwavg_d = d"], 10),
      RollingWavgTick("b", ["rwavg_a = a", "rwavg_d = d"], 10, 10),
      RollingWavgTime("Timestamp", "b", ["rwavg_b = b", "rwavg_e = e"], "PT00:00:10"),
      RollingWavgTime("Timestamp", "b", ["rwavg_b = b", "rwavg_e = e"], Secs(10), Secs(-10)),
      RollingWavgTime("Timestamp", "b", ["rwavg_b = b", "rwavg_e = e"], "PT30S", "-PT00:00:20")
    };
    return result;
  }
}
