//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;
using Xunit.Abstractions;

namespace Deephaven.Dh_NetClientTests;

public class SelectTest {
  private readonly ITestOutputHelper _output;

  public SelectTest(ITestOutputHelper output) {
    _output = output;
  }

  [Fact]
  public void TestSupportAllTypes() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var boolData = new List<bool>();
    var charData = new List<char>();
    var byteData = new List<sbyte>();
    var shortData = new List<short>();
    var intData = new List<int>();
    var longData = new List<long>();
    var floatData = new List<float>();
    var doubleData = new List<double>();
    var stringData = new List<string>();
    var dateTimeData = new List<DateTimeOffset>();
    var dateOnlyData = new List<DateOnly>();
    var timeOnlyData = new List<TimeOnly>();

    const int startValue = -8;
    const int endValue = 8;
    for (var i = startValue; i != endValue; ++i) {
      boolData.Add((i % 2) == 0);
      charData.Add((char)(i * 10));
      byteData.Add((sbyte)(i * 11));
      shortData.Add((short)(i * 1000));
      intData.Add(i * 1_000_000);
      longData.Add(i * (long)1_000_000_000);
      floatData.Add(i * 123.456F);
      doubleData.Add(i * 987654.321);
      stringData.Add($"test {i}");
      dateTimeData.Add(DateTimeOffset.FromUnixTimeMilliseconds(i));
      dateOnlyData.Add(DateOnly.FromDayNumber(i + 1000));
      timeOnlyData.Add(TimeOnly.FromTimeSpan(TimeSpan.FromMilliseconds(i + 1000)));
    }

    var tm = new TableMaker();
    tm.AddColumn("boolData", boolData);
    tm.AddColumn("charData", charData);
    tm.AddColumn("byteData", byteData);
    tm.AddColumn("shortData", shortData);
    tm.AddColumn("intData", intData);
    tm.AddColumn("longData", longData);
    tm.AddColumn("floatData", floatData);
    tm.AddColumn("doubleData", doubleData);
    tm.AddColumn("stringData", stringData);
    tm.AddColumn("dateTimeData", dateTimeData);
    tm.AddColumn("dateOnlyData", dateOnlyData);
    tm.AddColumn("timeOnlyData", timeOnlyData);

    var th = tm.MakeTable(ctx.Client.Manager);
    _output.WriteLine(th.ToString(true));

    TableComparer.AssertSame(tm, th);
  }

  [Fact]
  public void TestCreateUpdateFetchATable() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var tm = new TableMaker();
    tm.AddColumn("IntValue", [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    tm.AddColumn("DoubleValue", [0.0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9]);
    tm.AddColumn("StringValue", [
      "zero", "one", "two", "three", "four", "five", "six", "seven",
      "eight", "nine"
    ]);
    var t = tm.MakeTable(ctx.Client.Manager);
    var t2 = t.Update("Q2 = IntValue * 100");
    var t3 = t2.Update("Q3 = Q2 + 10");
    var t4 = t3.Update("Q4 = Q2 + 100");

    tm.AddColumn("Q2", [0, 100, 200, 300, 400, 500, 600, 700, 800, 900]);
    tm.AddColumn("Q3", [10, 110, 210, 310, 410, 510, 610, 710, 810, 910]);
    tm.AddColumn("Q4", [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]);
    TableComparer.AssertSame(tm, t4);
  }

  [Fact]
  public void TestSelectAFewColumns() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    var t1 = table.Where("ImportDate == `2017-11-01` && Ticker == `AAPL`")
      .Select("Ticker", "Close", "Volume")
      .Head(2);

    var tm = new TableMaker();
    tm.AddColumn("Ticker", ["AAPL", "AAPL"]);
    tm.AddColumn("Close", [23.5, 24.2]);
    tm.AddColumn("Volume", [(Int64)100000, 250000]);
    TableComparer.AssertSame(tm, t1);
  }


  [Fact]
  public void TestLastByAndSelect() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    var t1 = table.Where("ImportDate == `2017-11-01` && Ticker == `AAPL`")
      .LastBy("Ticker")
      .Select("Ticker", "Close", "Volume");
    _output.WriteLine(t1.ToString(true));

    var tm = new TableMaker();
    tm.AddColumn("Ticker", ["AAPL"]);
    tm.AddColumn("Close", [26.7]);
    tm.AddColumn("Volume", [(Int64)19000]);
    TableComparer.AssertSame(tm, t1);
  }

  [Fact]
  public void TestNewColumns() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    // A formula expression
    var t1 = table.Where("ImportDate == `2017-11-01` && Ticker == `AAPL`")
      .Select("MV1 = Volume * Close", "V_plus_12 = Volume + 12");

    var tm = new TableMaker();
    tm.AddColumn("MV1", [(double)2350000, 6050000, 507300]);
    tm.AddColumn("V_plus_12", [(Int64)100012, 250012, 19012]);
    TableComparer.AssertSame(tm, t1);
  }


  [Fact]
  public void TestDropColumns() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    var t1 = table.DropColumns("ImportDate", "Open", "Close");
    Assert.Equal(5, table.Schema.FieldsList.Count);
    Assert.Equal(2, t1.Schema.FieldsList.Count);
  }


  [Fact]
  public void TestSimpleWhere() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;
    var updated = table.Update("QQQ = i");

    var t1 = updated.Where("ImportDate == `2017-11-01` && Ticker == `IBM`")
      .Select("Ticker", "Volume");

    var tm = new TableMaker();
    tm.AddColumn("Ticker", ["IBM"]);
    tm.AddColumn("Volume", [(Int64)138000]);
    TableComparer.AssertSame(tm, t1);
  }

  [Fact]
  public void TestFormulaInTheWhereClause() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    var t1 = table.Where(
        "ImportDate == `2017-11-01` && Ticker == `AAPL` && Volume % 10 == Volume % 100")
      .Select("Ticker", "Volume");
    _output.WriteLine(t1.ToString(true));

    var tm = new TableMaker();
    tm.AddColumn("Ticker", ["AAPL", "AAPL", "AAPL"]);
    tm.AddColumn("Volume", [(Int64)100000, 250000, 19000]);
    TableComparer.AssertSame(tm, t1);
  }

  [Fact]
  public void TestSimpleWhereWithSyntaxError() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    Assert.Throws<AggregateException>(() => {
      var t1 = table.Where(")))))");
      _output.WriteLine(t1.ToString(true));
    });
  }

  [Fact]
  public void TestWhereIn() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var sourceMaker = new TableMaker();
    sourceMaker.AddColumn("Letter", ["A", "C", "F", "B", "E", "D", "A"]);
    sourceMaker.AddColumn("Number", [(Int32?)null, 2, 1, null, 4, 5, 3]);
    sourceMaker.AddColumn("Color", ["red", "blue", "orange", "purple", "yellow", "pink", "blue"]);
    sourceMaker.AddColumn("Code", [(Int32?)12, 13, 11, null, 16, 14, null]);

    var source = sourceMaker.MakeTable(ctx.Client.Manager);

    var filterMaker = new TableMaker();
    filterMaker.AddColumn("Colors", ["blue", "red", "purple", "white"]);
    var filter = filterMaker.MakeTable(ctx.Client.Manager);

    var result = source.WhereIn(filter, "Color = Colors");

    var expected = new TableMaker();
    expected.AddColumn("Letter", ["A", "C", "B", "A"]);
    expected.AddColumn("Number", [(Int32?)null, 2, null, 3]);
    expected.AddColumn("Color", ["red", "blue", "purple", "blue"]);
    expected.AddColumn("Code", [(Int32?)12, 13, null, null]);
    TableComparer.AssertSame(expected, result);
  }

  [Fact]
  public void TestLazyUpdate() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var sourceMaker = new TableMaker();
    sourceMaker.AddColumn("A", ["The", "At", "Is", "On"]);
    sourceMaker.AddColumn("B", [1, 2, 3, 4]);
    sourceMaker.AddColumn("C", [5, 2, 5, 5]);
    var source = sourceMaker.MakeTable(ctx.Client.Manager);

    var result = source.LazyUpdate("Y = sqrt(C)");

    var expected = new TableMaker();
    expected.AddColumn("A", ["The", "At", "Is", "On"]);
    expected.AddColumn("B", [1, 2, 3, 4]);
    expected.AddColumn("C", [5, 2, 5, 5]);
    expected.AddColumn("Y", [Math.Sqrt(5), Math.Sqrt(2), Math.Sqrt(5), Math.Sqrt(5)]);
    TableComparer.AssertSame(expected, result);
  }

  [Fact]
  public void TestSelectDistinct() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var sourceMaker = new TableMaker();
    sourceMaker.AddColumn("A", ["apple", "apple", "orange", "orange", "plum", "grape"]);
    sourceMaker.AddColumn("B", [1, 1, 2, 2, 3, 3]);
    var source = sourceMaker.MakeTable(ctx.Client.Manager);

    var result = source.SelectDistinct("A");
    _output.WriteLine(result.ToString(true));

    var expected = new TableMaker();
    expected.AddColumn("A", ["apple", "orange", "plum", "grape"]);
    TableComparer.AssertSame(expected, result);
  }
}
