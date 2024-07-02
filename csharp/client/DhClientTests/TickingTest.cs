using Deephaven.DeephavenClient;
using Microsoft.VisualBasic;
using System.Runtime.InteropServices;
using System;
using Xunit.Abstractions;
using static System.Reflection.Metadata.BlobBuilder;

namespace Deephaven.DhClientTests;

public class TickingTest {
  private readonly ITestOutputHelper _output;

  public TickingTest(ITestOutputHelper output) {
    _output = output;
  }

  [Fact]
  public void EventuallyReaches10Rows() {
    const Int64 maxRows = 10;
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;

    using var table = thm.TimeTable(TimeSpan.FromMilliseconds(500)).Update("II = ii");
    var callback = new ReachesNRowsCallback(_output, maxRows);
    using var cookie = table.Subscribe(callback);

    while (true) {
      var (done, errorText) = callback.WaitForUpdate();
      if (done) {
        break;
      }
      if (errorText != null) {
        throw new Exception(errorText);
      }
    }

    table.Unsubscribe(cookie);
  }

  [Fact]
  public void AFourthTest() {
    Assert.Equal(7, 7);
  }

  [Fact]
  public void AFifthTest() {
    Assert.Equal(7, 7);
  }

  [Fact]
  public void AllEventuallyGreaterThan10() {
    const Int64 maxRows = 10;
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;

    using var table = thm.TimeTable(TimeSpan.FromMilliseconds(500))
      .View("Key = (long)(ii % 10)", "Value = ii")
      .LastBy("Key");

    var callback = new AllValuesGreaterThanNCallback(_output, maxRows);
    using var cookie = table.Subscribe(callback);

    while (true) {
      var (done, errorText) = callback.WaitForUpdate();
      if (done) {
        break;
      }
      if (errorText != null) {
        throw new Exception(errorText);
      }
    }

    table.Unsubscribe(cookie);
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
        "DateTimes = II == 5 ? null : ('2001-03-01T12:34:56Z' + II)")
      .LastBy("II")
      .Sort(SortPair.Ascending("II"))
      .DropColumns("Timestamp", "II");

    var callback = new WaitForPopulatedTableCallback(_output, maxRows);
    using var cookie = table.Subscribe(callback);

    while (true) {
      var (done, errorText) = callback.WaitForUpdate();
      if (done) {
        break;
      }
      if (errorText != null) {
        throw new Exception(errorText);
      }
    }

    table.Unsubscribe(cookie);
  }
}

public abstract class CommonBase : ITickingCallback {
  private readonly object _sync = new();
  private bool _done = false;
  private string? _errorText = null;

  public void OnFailure(string errorText) {
    lock (_sync) {
      _errorText = errorText;
      Monitor.PulseAll(_sync);
    }
  }

  public (bool, string?) WaitForUpdate() {
    lock (_sync) {
      while (true) {
        if (_done || _errorText != null) {
          return (_done, _errorText);
        }

        Monitor.Wait(_sync);
      }
    }
  }

  public abstract void OnTick(TickingUpdate update);

  protected void NotifyDone() {
    lock (_sync) {
      _done = true;
      Monitor.PulseAll(_sync);
    }
  }
}

public sealed class ReachesNRowsCallback : CommonBase {
  private readonly ITestOutputHelper _output;
  private readonly Int64 _targetRows;

  public ReachesNRowsCallback(ITestOutputHelper output, Int64 targetRows) {
    _output = output;
    _targetRows = targetRows;
  }

  public override void OnTick(TickingUpdate update) {
    _output.WriteLine($"=== The Full Table ===\n{update.Current.ToString(true, true)}");
    if (update.Current.NumRows >= _targetRows) {
      NotifyDone();
    }
  }
}

public sealed class WaitForPopulatedTableCallback : CommonBase {
  private readonly ITestOutputHelper _output;
  private readonly Int64 _target;

  public WaitForPopulatedTableCallback(ITestOutputHelper output, Int64 target) {
    _output = output;
    _target = target;
  }

  public override void OnTick(TickingUpdate update) {
    _output.WriteLine($"=== The Full Table ===\n{update.Current.ToString(true, true)}");

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
    var dateTimeData = new List<DhDateTime?>();

    var dateTimeStart = DhDateTime.Parse("2001-03-01T12:34:56Z");

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
      dateTimeData.Add(DhDateTime.FromNanos(dateTimeStart.Nanos + i));
    }

    if (_target == 0) {
      throw new Exception("Target should not be 0");
    }

    var t2 = (int)(_target / 2);
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

    var tc = new TableComparer();
    tc.AddColumn("Chars", charData);
    tc.AddColumn("Bytes", int8Data);
    tc.AddColumn("Shorts", int16Data);
    tc.AddColumn("Ints", int32Data);
    tc.AddColumn("Longs", int64Data);
    tc.AddColumn("Floats", floatData);
    tc.AddColumn("Doubles", doubleData);
    tc.AddColumn("Bools", boolData);
    tc.AddColumn("Strings", stringData);
    tc.AddColumn("DateTimes", dateTimeData);

    tc.AssertEqualTo(current);
    NotifyDone();
  }
}

public sealed class AllValuesGreaterThanNCallback : CommonBase {
  private readonly ITestOutputHelper _output;
  private readonly Int64 _target;

  public AllValuesGreaterThanNCallback(ITestOutputHelper output, Int64 target) {
    _output = output;
    _target = target;
  }

  public override void OnTick(TickingUpdate update) {
    _output.WriteLine($"=== The Full Table ===\n{update.Current.ToString(true, true)}");

    var current = update.Current;

    if (current.NumRows == 0) {
      return;
    }

    var (values, nulls) = current.GetColumn("Value");
    var data = (Int64[])values;
    var allGreater = data.All(elt => elt > _target);
    if (allGreater) {
      NotifyDone();
    }
  }
}
