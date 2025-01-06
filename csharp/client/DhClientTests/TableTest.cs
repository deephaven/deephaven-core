using Deephaven.DeephavenClient;
using Deephaven.DeephavenClient.Utility;
using System;
using Xunit.Abstractions;

namespace Deephaven.DhClientTests;

public class TableTest {
  [Fact]
  public void FetchTheWholeTable() {
    const int target = 10;
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;
    var th = thm.EmptyTable(target)
      .Update(
        "Chars = ii == 5 ? null : (char)('a' + ii)",
        "Bytes = ii == 5 ? null : (byte)(ii)",
        "Shorts = ii == 5 ? null : (short)(ii)",
        "Ints = ii == 5 ? null : (int)(ii)",
        "Longs = ii == 5 ? null : (long)(ii)",
        "Floats = ii == 5 ? null : (float)(ii)",
        "Doubles = ii == 5 ? null : (double)(ii)",
        "Bools = ii == 5 ? null : ((ii % 2) == 0)",
        "Strings = ii == 5 ? null : `hello ` + i",
        "DateTimes = ii == 5 ? null : '2001-03-01T12:34:56Z' + ii"
      );

    var chars = new List<char?>();
    var int8s = new List<sbyte?>();
    var int16s = new List<Int16?>();
    var int32s = new List<Int32?>();
    var int64s = new List<Int64?>();
    var floats = new List<float?>();
    var doubles = new List<double?>();
    var bools = new List<bool?>();
    var strings = new List<string?>();
    var dateTimes = new List<DhDateTime?>();

    var dateTimeStart = DhDateTime.Parse("2001-03-01T12:34:56Z");

    for (var i = 0; i != target; ++i) {
      chars.Add((char)('a' + i));
      int8s.Add((sbyte)i);
      int16s.Add((Int16)i);
      int32s.Add((Int32)i);
      int64s.Add((Int64)i);
      floats.Add((float)i);
      doubles.Add((double)i);
      bools.Add((i % 2) == 0);
      strings.Add($"hello {i}");
      dateTimes.Add(DhDateTime.FromNanos(dateTimeStart.Nanos + i));
    }

    var t2 = target / 2;
    // Set the middle element to the null
    chars[t2] = null;
    int8s[t2] = null;
    int16s[t2] = null;
    int32s[t2] = null;
    int64s[t2] = null;
    floats[t2] = null;
    doubles[t2] = null;
    bools[t2] = null;
    strings[t2] = null;
    dateTimes[t2] = null;

    var tc = new TableComparer();
    tc.AddColumn("Chars", chars);
    tc.AddColumn("Bytes", int8s);
    tc.AddColumn("Shorts", int16s);
    tc.AddColumn("Ints", int32s);
    tc.AddColumn("Longs", int64s);
    tc.AddColumn("Floats", floats);
    tc.AddColumn("Doubles", doubles);
    tc.AddColumn("Bools", bools);
    tc.AddColumn("Strings", strings);
    tc.AddColumn("DateTimes", dateTimes);
    tc.AssertEqualTo(th);
  }
}
