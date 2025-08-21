//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

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
        "DateTimes = ii == 5 ? null : '2001-03-01T12:34:56Z' + ii * 1000000"
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
    var dateTimes = new List<DateTimeOffset?>();

    var dateTimeStart = DateTimeOffset.Parse("2001-03-01T12:34:56Z");

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
      dateTimes.Add(DateTimeOffset.FromUnixTimeMilliseconds(dateTimeStart.ToUnixTimeMilliseconds() + i));
    }

    var t2 = target / 2;
    // Set the middle element to the null value
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

    var expected = new TableMaker();
    expected.AddColumn("Chars", chars);
    expected.AddColumn("Bytes", int8s);
    expected.AddColumn("Shorts", int16s);
    expected.AddColumn("Ints", int32s);
    expected.AddColumn("Longs", int64s);
    expected.AddColumn("Floats", floats);
    expected.AddColumn("Doubles", doubles);
    expected.AddColumn("Bools", bools);
    expected.AddColumn("Strings", strings);
    expected.AddColumn("DateTimes", dateTimes);

    TableComparer.AssertSame(expected, th);
  }
}
