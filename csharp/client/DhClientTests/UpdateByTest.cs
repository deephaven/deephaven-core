using Deephaven.DeephavenClient;
using Deephaven.DeephavenClient.Utility;
using System;
using Deephaven.DeephavenClient.UpdateBy;
using Xunit.Abstractions;
using static Deephaven.DeephavenClient.UpdateBy.UpdateByOperation;

namespace Deephaven.DhClientTests;

public class UpdateByTest {
  const int NumCols = 5;
  const int NumRows = 1000;

  private readonly ITestOutputHelper _output;

  public UpdateByTest(ITestOutputHelper output) {
    _output = output;
  }

  [Fact]
  public void SimpleCumSum() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;

    var source = tm.EmptyTable(10).Update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i");
    var result = source.UpdateBy(new[] { CumSum(new[] { "SumX = X" }) }, new[] { "Letter" });
    var filtered = result.Select("SumX");
    var tc = new TableComparer();
    tc.AddColumn("SumX", new Int64[] { 0, 1, 2, 4, 6, 9, 12, 16, 20, 25 });
    tc.AssertEqualTo(filtered);
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
        _output.WriteLine($"Processing op {opIndex} on Table {tableIndex}");
        using var result = table.UpdateBy(new[] { op }, new[] { "e" });
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
        _output.WriteLine($"Processing op {opIndex} on Table {tableIndex}");
        using var result = table.UpdateBy(new[] { op }, new[] { "b" });
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
        _output.WriteLine($"Processing op {opIndex} on Table {tableIndex}");
        using var result = table.UpdateBy(new[] { op }, new[] { "c" });
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
      CumSum(new[] { "sum_a=a", "sum_b=b" }),
      CumSum(new[] { "max_a=a", "max_d=d" }),
      EmaTick(10, new[] { "ema_d=d", "ema_e=e" }),
      EmaTime("Timestamp", "PT00:00:00.1", new[] { "ema_time_d=d", "ema_time_e=e" }),
      RollingWavgTick("b", new[] { "rwavg_a = a", "rwavg_d = d" }, 10)
    };

    for (var tableIndex = 0; tableIndex != tables.Length; ++tableIndex) {
      var table = tables[tableIndex];
      _output.WriteLine($"Processing table {tableIndex}");
      using var result = table.UpdateBy(multipleOps, new[] { "c" });
      Assert.Equal(table.IsStatic, result.IsStatic);
      Assert.Equal(10 + table.NumCols, result.NumCols);
      if (result.IsStatic) {
        Assert.Equal(result.NumRows, table.NumRows);
      }
    }
  }

  private TableHandle[] MakeTables(TableHandleManager tm) {
    var staticTable = MakeRandomTable(tm).Update("Timestamp=now()");
    var tickingTable = tm.TimeTable(TimeSpan.FromSeconds(1))
      .Update("a = i", "b = i*i % 13", "c = i * 13 % 23", "d = a + b", "e = a - b");
    return new[] { staticTable, tickingTable };
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
      EmaTick(100, new[] { "ema_a = a" }),
      EmaTick(100, new[] { "ema_a = a" }, emOpControl),
      EmaTime("Timestamp", 10, new[] { "ema_a = a" }),
      EmaTime("Timestamp", "PT00:00:00.001", new[] { "ema_c = c" }, emOpControl),
      EmaTime("Timestamp", "PT1M", new[] { "ema_c = c" }),
      EmaTime("Timestamp", "PT1M", new[] { "ema_c = c" }, emOpControl),
      // exponential moving sum
      EmsTick(100, new[] { "ems_a = a" }),
      EmsTick(100, new[] { "ems_a = a" }, emOpControl),
      EmsTime("Timestamp", 10,  new[] { "ems_a = a"}),
      EmsTime("Timestamp", "PT00:00:00.001", new[] { "ems_c = c" }, emOpControl),
      EmsTime("Timestamp", "PT1M", new[] { "ema_c = c" }),
      EmsTime("Timestamp", "PT1M", new[] { "ema_c = c" }, emOpControl),
      // exponential moving minimum
      EmminTick(100, new[] { "emmin_a = a" }),
      EmminTick(100, new[] { "emmin_a = a" }, emOpControl),
      EmminTime("Timestamp", 10, new[] { "emmin_a = a" }),
      EmminTime("Timestamp", "PT00:00:00.001", new[] { "emmin_c = c" }, emOpControl),
      EmminTime("Timestamp", "PT1M", new[] { "ema_c = c" }),
      EmminTime("Timestamp", "PT1M", new[] { "ema_c = c" }, emOpControl),
      // exponential moving maximum
      EmmaxTick(100, new[] { "emmax_a = a" }),
      EmmaxTick(100, new[] { "emmax_a = a" }, emOpControl),
      EmmaxTime("Timestamp", 10, new[] { "emmax_a = a" }),
      EmmaxTime("Timestamp", "PT00:00:00.001", new[] { "emmax_c = c" }, emOpControl),
      EmmaxTime("Timestamp", "PT1M", new[] { "ema_c = c" }),
      EmmaxTime("Timestamp", "PT1M", new[] { "ema_c = c" }, emOpControl),
      // exponential moving standard deviation
      EmstdTick(100, new[] { "emstd_a = a" }),
      EmstdTick(100, new[] { "emstd_a = a" }, emOpControl),
      EmstdTime("Timestamp", 10, new[] { "emstd_a = a" }),
      EmstdTime("Timestamp", "PT00:00:00.001", new[] { "emtd_c = c" }, emOpControl),
      EmstdTime("Timestamp", "PT1M", new[] { "ema_c = c" }),
      EmstdTime("Timestamp", "PT1M", new[] { "ema_c = c" }, emOpControl)
    };

    return result;
  }

  private static UpdateByOperation[] MakeRollingOps() {
    static TimeSpan Secs(int s) => TimeSpan.FromSeconds(s);

    // exponential moving average
    var result = new UpdateByOperation[] {
      // rolling sum
      RollingSumTick(new[] {"rsum_a = a", "rsum_d = d"}, 10),
      RollingSumTick(new[] { "rsum_a = a", "rsum_d = d"}, 10, 10),
      RollingSumTime("Timestamp",  new[] { "rsum_b = b", "rsum_e = e"}, "PT00:00:10"),
      RollingSumTime("Timestamp",  new[] { "rsum_b = b", "rsum_e = e"}, Secs(10), Secs(-10)),
      RollingSumTime("Timestamp",  new[] { "rsum_b = b", "rsum_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling group
      RollingGroupTick(new[] { "rgroup_a = a", "rgroup_d = d"}, 10),
      RollingGroupTick(new[] { "rgroup_a = a", "rgroup_d = d"}, 10, 10),
      RollingGroupTime("Timestamp", new[] { "rgroup_b = b", "rgroup_e = e"}, "PT00:00:10"),
      RollingGroupTime("Timestamp", new[] { "rgroup_b = b", "rgroup_e = e"}, Secs(10), Secs(-10)),
      RollingGroupTime("Timestamp",  new[] { "rgroup_b = b", "rgroup_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling average
      RollingAvgTick(new[] { "ravg_a = a", "ravg_d = d"}, 10),
      RollingAvgTick(new[] { "ravg_a = a", "ravg_d = d"}, 10, 10),
      RollingAvgTime("Timestamp", new[] { "ravg_b = b", "ravg_e = e"}, "PT00:00:10"),
      RollingAvgTime("Timestamp", new[] { "ravg_b = b", "ravg_e = e"}, Secs(10), Secs(-10)),
      RollingAvgTime("Timestamp",  new[] { "ravg_b = b", "ravg_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling minimum
      RollingMinTick(new[] { "rmin_a = a", "rmin_d = d"}, 10),
      RollingMinTick(new[] { "rmin_a = a", "rmin_d = d"}, 10, 10),
      RollingMinTime("Timestamp", new[] { "rmin_b = b", "rmin_e = e"}, "PT00:00:10"),
      RollingMinTime("Timestamp", new[] { "rmin_b = b", "rmin_e = e"}, Secs(10), Secs(-10)),
      RollingMinTime("Timestamp", new[] { "rmin_b = b", "rmin_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling maximum
      RollingMaxTick(new[] { "rmax_a = a", "rmax_d = d"}, 10),
      RollingMaxTick(new[] { "rmax_a = a", "rmax_d = d"}, 10, 10),
      RollingMaxTime("Timestamp", new[] { "rmax_b = b", "rmax_e = e"}, "PT00:00:10"),
      RollingMaxTime("Timestamp", new[] { "rmax_b = b", "rmax_e = e"}, Secs(10), Secs(-10)),
      RollingMaxTime("Timestamp", new[] { "rmax_b = b", "rmax_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling product
      RollingProdTick(new[] { "rprod_a = a", "rprod_d = d"}, 10),
      RollingProdTick(new[] { "rprod_a = a", "rprod_d = d"}, 10, 10),
      RollingProdTime("Timestamp", new[] { "rprod_b = b", "rprod_e = e"}, "PT00:00:10"),
      RollingProdTime("Timestamp", new[] { "rprod_b = b", "rprod_e = e"}, Secs(10), Secs(-10)),
      RollingProdTime("Timestamp",  new[] { "rprod_b = b", "rprod_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling count
      RollingCountTick(new[] { "rcount_a = a", "rcount_d = d"}, 10),
      RollingCountTick(new[] { "rcount_a = a", "rcount_d = d"}, 10, 10),
      RollingCountTime("Timestamp", new[] { "rcount_b = b", "rcount_e = e"}, "PT00:00:10"),
      RollingCountTime("Timestamp",  new[] { "rcount_b = b", "rcount_e = e"}, Secs(10), Secs(-10)),
      RollingCountTime("Timestamp", new[] { "rcount_b = b", "rcount_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling standard deviation
      RollingStdTick(new[] { "rstd_a = a", "rstd_d = d"}, 10),
      RollingStdTick(new[] { "rstd_a = a", "rstd_d = d"}, 10, 10),
      RollingStdTime("Timestamp", new[] { "rstd_b = b", "rstd_e = e"}, "PT00:00:10"),
      RollingStdTime("Timestamp", new[] { "rstd_b = b", "rstd_e = e"}, Secs(10), Secs(-10)),
      RollingStdTime("Timestamp", new[] { "rstd_b = b", "rstd_e = e"}, "PT30S", "-PT00:00:20"),
      // rolling weighted average (using "b" as the weight column)
      RollingWavgTick("b", new[] { "rwavg_a = a", "rwavg_d = d"}, 10),
      RollingWavgTick("b", new[] { "rwavg_a = a", "rwavg_d = d"}, 10, 10),
      RollingWavgTime("Timestamp", "b", new[] { "rwavg_b = b", "rwavg_e = e"}, "PT00:00:10"),
      RollingWavgTime("Timestamp", "b", new[] { "rwavg_b = b", "rwavg_e = e"}, Secs(10), Secs(-10)),
      RollingWavgTime("Timestamp", "b", new[] { "rwavg_b = b", "rwavg_e = e"}, "PT30S", "-PT00:00:20")
    };
    return result;
  }
}
