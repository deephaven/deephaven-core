//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Apache.Arrow;
using Deephaven.Dh_NetClient;
using Xunit.Abstractions;

namespace Deephaven.Dh_NetClientTests;

public class TickingTest(ITestOutputHelper output) {
  [Fact]
  public void EventuallyReaches10Rows() {
    const Int64 maxRows = 10;
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;

    using var table = thm.TimeTable(TimeSpan.FromMilliseconds(500)).Update("II = ii");
    var callback = new ReachesNRowsCallback(output, maxRows);
    using var cookie = table.Subscribe(callback);

    while (true) {
      var (done, exception) = callback.WaitForUpdate();
      if (done) {
        break;
      }
      if (exception != null) {
        throw new Exception("Caught exception", exception);
      }
    }
  }

  [Fact]
  public void AllEventuallyGreaterThan10() {
    const Int64 maxRows = 10;
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;

    using var table = thm.TimeTable(TimeSpan.FromMilliseconds(500))
      .View("Key = (long)(ii % 10)", "Value = ii")
      .LastBy("Key");

    var callback = new AllValuesGreaterThanNCallback(output, maxRows);
    using var cookie = table.Subscribe(callback);

    while (true) {
      var (done, exception) = callback.WaitForUpdate();
      if (done) {
        break;
      }
      if (exception != null) {
        throw new Exception("Caught exception", exception);
      }
    }
  }

  [Fact]
  public void AllDataEventuallyPresent() {
    const Int64 maxRows = 10;
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;

    using var table = thm.TimeTable("PT0:00:0.5")
      .Update(
        "II = (int)((ii * 7) % 10)",
        "Chars = II == 5 ? null : (char)(II + 'a')",
        "Bytes = II == 5 ? null : (byte)II",
        "Shorts = II == 5 ? null : (short)II",
        "Ints = II == 5 ? null : (int)II",
        "Longs = II == 5 ? null : (long)II",
        "Floats = II == 5 ? null : (float)II",
        "Doubles = II == 5 ? null : (double)II",
        "Bools = II == 5 ? null : ((II % 2) == 0)",
        "Strings = II == 5 ? null : (`hello ` + II)",
        "DateTimes = II == 5 ? null : ('2001-03-01T12:34:56Z' + II * 1000)")
      .LastBy("II")
      .Sort(SortPair.Ascending("II"))
      .DropColumns("Timestamp", "II");

    var callback = new WaitForPopulatedTableCallback(output, maxRows);
    using var cookie = table.Subscribe(callback);

    while (true) {
      var (done, exception) = callback.WaitForUpdate();
      if (done) {
        break;
      }
      if (exception != null) {
        throw new Exception("Caught exception", exception);
      }
    }
  }
}

public abstract class CommonBase : IObserver<TickingUpdate> {
  protected readonly ITestOutputHelper Output;

  protected CommonBase(ITestOutputHelper output) { 
    Output = output;
  }

  private readonly object _sync = new();
  private bool _done = false;
  private Exception? _exception = null;

  public void OnError(Exception error) {
    lock (_sync) {
      _exception = error;
      Monitor.PulseAll(_sync);
    }
  }

  public (bool, Exception?) WaitForUpdate() {
    lock (_sync) {
      while (true) {
        if (_done || _exception != null) {
          return (_done, _exception);
        }

        Monitor.Wait(_sync);
      }
    }
  }

  public void OnCompleted() {
    Output.WriteLine("Subscription complete");
  }

  public abstract void OnNext(TickingUpdate value);

  protected void NotifyDone() {
    lock (_sync) {
      _done = true;
      Monitor.PulseAll(_sync);
    }
  }
}

public sealed class ReachesNRowsCallback: CommonBase {
  private readonly Int64 _target;

  public ReachesNRowsCallback(ITestOutputHelper output, Int64 target) : base(output) {
    _target = target;
  }

  public override void OnNext(TickingUpdate update) {
    Output.WriteLine($"=== The Full Table ===\n{update.Current.ToString(true, true)}");
    if (update.Current.NumRows >= _target) {
      NotifyDone();
    }
  }
}

public sealed class WaitForPopulatedTableCallback : CommonBase {
  private readonly Int64 _target;

  public WaitForPopulatedTableCallback(ITestOutputHelper output, Int64 target) : base(output) {
    _target = target;
  }

  public override void OnNext(TickingUpdate update) {
    Output.WriteLine($"=== The Full Table ===\n{update.Current.ToString(true, true)}");

    var current = update.Current;

    if (current.NumRows < _target) {
      return;
    }

    if (current.NumRows > _target) {
      // table has more rows than expected.
      var message = $"Expected table to have {_target} rows, got {current.NumRows}";
      throw new Exception(message);
    }

    var charData = new List<char?>();
    var int8Data = new List<sbyte?>();
    var int16Data = new List<short?>();
    var int32Data = new List<int?>();
    var int64Data = new List<long?>();
    var floatData = new List<float?>();
    var doubleData = new List<double?>();
    var boolData = new List<bool?>();
    var stringData = new List<string?>();
    var dateTimeData = new List<DateTimeOffset?>();

    var dateTimeStart = DateTime.Parse("2001-03-01T12:34:56Z");

    for (Int64 i = 0; i != _target; ++i) {
      charData.Add((char)('a' + i));
      int8Data.Add((sbyte)(i));
      int16Data.Add((Int16)(i));
      int32Data.Add((Int32)(i));
      int64Data.Add((Int64)(i));
      floatData.Add((float)(i));
      doubleData.Add((double)(i));
      boolData.Add((i % 2) == 0);
      stringData.Add($"hello {i}");
      dateTimeData.Add(dateTimeStart.AddMicroseconds(i));
    }

    if (_target == 0) {
      throw new Exception("Target should not be 0");
    }

    var t2 = (_target / 2).ToIntExact();
    // Set the middle element to the unset optional, which for the purposes of this test is
    // our representation of null.
    charData[t2] = null;
    int8Data[t2] = null;
    int16Data[t2] = null;
    int32Data[t2] = null;
    int64Data[t2] = null;
    floatData[t2] = null;
    doubleData[t2] = null;
    boolData[t2] = null;
    stringData[t2] = null;
    dateTimeData[t2] = null;

    var expected = new TableMaker();
    expected.AddColumn("Chars", charData);
    expected.AddColumn("Bytes", int8Data);
    expected.AddColumn("Shorts", int16Data);
    expected.AddColumn("Ints", int32Data);
    expected.AddColumn("Longs", int64Data);
    expected.AddColumn("Floats", floatData);
    expected.AddColumn("Doubles", doubleData);
    expected.AddColumn("Bools", boolData);
    expected.AddColumn("Strings", stringData);
    expected.AddColumn("DateTimes", dateTimeData);

    TableComparer.AssertSame(expected, current);
    NotifyDone();
  }
}

public sealed class AllValuesGreaterThanNCallback : CommonBase {
  private readonly Int64 _target;

  public AllValuesGreaterThanNCallback(ITestOutputHelper output, Int64 target) : base(output) {
    _target = target;
  }

  public override void OnNext(TickingUpdate update) {
    var current = update.Current;

    Output.WriteLine($"=== The Full Table ===\n{current.ToString(true, true)}");

    if (current.NumRows == 0) {
      return;
    }

    var col = current.GetColumn("Value");
    var data = (Int64Array)ArrowArrayConverter.ColumnSourceToArray(col, current.NumRows);
    var allGreater = data.All(elt => elt > _target);
    if (allGreater) {
      NotifyDone();
    }
  }
}
