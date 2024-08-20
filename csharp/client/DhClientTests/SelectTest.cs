using Deephaven.DeephavenClient.Interop;
using System;
using Deephaven.DeephavenClient;
using Deephaven.DeephavenClient.Utility;
using Xunit.Abstractions;

namespace Deephaven.DhClientTests;

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
    var dateTimeData = new List<DhDateTime>();

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
      dateTimeData.Add(DhDateTime.FromNanos(i));
    }

    using var maker = new TableMaker();
    maker.AddColumn("boolData", boolData);
    maker.AddColumn("charData", charData);
    maker.AddColumn("byteData", byteData);
    maker.AddColumn("shortData", shortData);
    maker.AddColumn("intData", intData);
    maker.AddColumn("longData", longData);
    maker.AddColumn("floatData", floatData);
    maker.AddColumn("doubleData", doubleData);
    maker.AddColumn("stringData", stringData);
    maker.AddColumn("dateTimeData", dateTimeData);

    var t = maker.MakeTable(ctx.Client.Manager);

    _output.WriteLine(t.ToString(true));

    var tc = new TableComparer();
    tc.AddColumn("boolData", boolData);
    tc.AddColumn("charData", charData);
    tc.AddColumn("byteData", byteData);
    tc.AddColumn("shortData", shortData);
    tc.AddColumn("intData", intData);
    tc.AddColumn("longData", longData);
    tc.AddColumn("floatData", floatData);
    tc.AddColumn("doubleData", doubleData);
    tc.AddColumn("stringData", stringData);
    tc.AddColumn("dateTimeData", dateTimeData);
    tc.AssertEqualTo(t);
  }

  [Fact]
  public void TestCreateUpdateFetchATable() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var intData = new[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    var doubleData = new[] { 0.0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9 };
    var stringData = new[] {
      "zero", "one", "two", "three", "four", "five", "six", "seven",
      "eight", "nine"
    };

    using var maker = new TableMaker();
    maker.AddColumn("IntValue", intData);
    maker.AddColumn("DoubleValue", doubleData);
    maker.AddColumn("StringValue", stringData);
    var t = maker.MakeTable(ctx.Client.Manager);
    var t2 = t.Update("Q2 = IntValue * 100");
    var t3 = t2.Update("Q3 = Q2 + 10");
    var t4 = t3.Update("Q4 = Q2 + 100");

    var q2Data = new[] { 0, 100, 200, 300, 400, 500, 600, 700, 800, 900 };
    var q3Data = new[] { 10, 110, 210, 310, 410, 510, 610, 710, 810, 910 };
    var q4Data = new[] { 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000 };

    var tc = new TableComparer();
    tc.AddColumn("IntValue", intData);
    tc.AddColumn("DoubleValue", doubleData);
    tc.AddColumn("StringValue", stringData);
    tc.AddColumn("Q2", q2Data);
    tc.AddColumn("Q3", q3Data);
    tc.AddColumn("Q4", q4Data);
    tc.AssertEqualTo(t4);
  }

  [Fact]
  public void TestSelectAFewColumns() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    var t1 = table.Where("ImportDate == `2017-11-01` && Ticker == `AAPL`")
      .Select("Ticker", "Close", "Volume")
      .Head(2);

    var tickerData = new[] { "AAPL", "AAPL" };
    var closeData = new[] { 23.5, 24.2 };
    var volData = new Int64[] { 100000, 250000 };

    var tc = new TableComparer();
    tc.AddColumn("Ticker", tickerData);
    tc.AddColumn("Close", closeData);
    tc.AddColumn("Volume", volData);
    tc.AssertEqualTo(t1);
  }

  [Fact]
  public void TestLastByAndSelect() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    var t1 = table.Where("ImportDate == `2017-11-01` && Ticker == `AAPL`")
      .LastBy("Ticker")
      .Select("Ticker", "Close", "Volume");
    _output.WriteLine(t1.ToString(true));

    var tickerData = new[] { "AAPL" };
    var closeData = new[] { 26.7 };
    var volData = new Int64[] { 19000 };

    var tc = new TableComparer();
    tc.AddColumn("Ticker", tickerData);
    tc.AddColumn("Close", closeData);
    tc.AddColumn("Volume", volData);
    tc.AssertEqualTo(t1);
  }

  [Fact]
  public void TestNewColumns() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    // A formula expression
    var t1 = table.Where("ImportDate == `2017-11-01` && Ticker == `AAPL`")
      .Select("MV1 = Volume * Close", "V_plus_12 = Volume + 12");

    var mv1Data = new double[] { 2350000, 6050000, 507300 };
    var vp2Data = new long[] { 100012, 250012, 19012 };

    var tc = new TableComparer();
    tc.AddColumn("MV1", mv1Data);
    tc.AddColumn("V_plus_12", vp2Data);
    tc.AssertEqualTo(t1);
  }

  [Fact]
  public void TestDropColumns() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    var t1 = table.DropColumns("ImportDate", "Open", "Close");
    Assert.Equal(5, table.Schema.NumCols);
    Assert.Equal(2, t1.Schema.NumCols);
  }

  [Fact]
  public void TestSimpleWhere() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;
    var updated = table.Update("QQQ = i");

    var t1 = updated.Where("ImportDate == `2017-11-01` && Ticker == `IBM`")
      .Select("Ticker", "Volume");

    var tickerData = new[] { "IBM" };
    var volData = new Int64[] { 138000 };

    var tc = new TableComparer();
    tc.AddColumn("Ticker", tickerData);
    tc.AddColumn("Volume", volData);
    tc.AssertEqualTo(t1);
  }

  [Fact]
  public void TestFormulaInTheWhereClause() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    var t1 = table.Where(
        "ImportDate == `2017-11-01` && Ticker == `AAPL` && Volume % 10 == Volume % 100")
      .Select("Ticker", "Volume");
    _output.WriteLine(t1.ToString(true));

    var tickerData = new[] { "AAPL", "AAPL", "AAPL" };
    var volData = new Int64[] { 100000, 250000, 19000 };

    var tc = new TableComparer();
    tc.AddColumn("Ticker", tickerData);
    tc.AddColumn("Volume", volData);
    tc.AssertEqualTo(t1);
  }

  [Fact]
  public void TestSimpleWhereWithSyntaxError() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    Assert.Throws<Exception>(() => {
      var t1 = table.Where(")))))");
      _output.WriteLine(t1.ToString(true));
    });
  }

  [Fact]
  public void TestWhereIn() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var letterData = new[] { "A", "C", "F", "B", "E", "D", "A" };
    var numberData = new Int32?[] { null, 2, 1, null, 4, 5, 3 };
    var colorData = new[] { "red", "blue", "orange", "purple", "yellow", "pink", "blue" };
    var codeData = new Int32?[] { 12, 13, 11, null, 16, 14, null };

    using var sourceMaker = new TableMaker();
    sourceMaker.AddColumn("Letter", letterData);
    sourceMaker.AddColumn("Number", numberData);
    sourceMaker.AddColumn("Color", colorData);
    sourceMaker.AddColumn("Code", codeData);

    var source = sourceMaker.MakeTable(ctx.Client.Manager);

    var filterColorData = new[] { "blue", "red", "purple", "white" };

    using var filterMaker = new TableMaker();
    filterMaker.AddColumn("Colors", filterColorData);
    var filter = filterMaker.MakeTable(ctx.Client.Manager);

    var result = source.WhereIn(filter, "Color = Colors");

    var letterExpected = new[] { "A", "C", "B", "A" };
    var numberExpected = new Int32?[] { null, 2, null, 3 };
    var colorExpected = new[] { "red", "blue", "purple", "blue" };
    var codeExpected = new Int32?[] { 12, 13, null, null };

    var expected = new TableComparer();
    expected.AddColumn("Letter", letterExpected);
    expected.AddColumn("Number", numberExpected);
    expected.AddColumn("Color", colorExpected);
    expected.AddColumn("Code", codeExpected);

    expected.AssertEqualTo(result);
  }

  [Fact]
  public void TestLazyUpdate() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var aData = new[] { "The", "At", "Is", "On" };
    var bData = new[] { 1, 2, 3, 4 };
    var cData = new[] { 5, 2, 5, 5 };

    using var sourceMaker = new TableMaker();
    sourceMaker.AddColumn("A", aData);
    sourceMaker.AddColumn("B", bData);
    sourceMaker.AddColumn("C", cData);
    var source = sourceMaker.MakeTable(ctx.Client.Manager);

    var result = source.LazyUpdate("Y = sqrt(C)");

    var sqrtData = new[] { Math.Sqrt(5), Math.Sqrt(2), Math.Sqrt(5), Math.Sqrt(5) };

    var tc = new TableComparer();
    tc.AddColumn("A", aData);
    tc.AddColumn("B", bData);
    tc.AddColumn("C", cData);
    tc.AddColumn("Y", sqrtData);
    tc.AssertEqualTo(result);
  }

  [Fact]
  public void TestSelectDistinct() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var aData = new[] { "apple", "apple", "orange", "orange", "plum", "grape" };
    var bData = new[] { 1, 1, 2, 2, 3, 3 };

    using var sourceMaker = new TableMaker();
    sourceMaker.AddColumn("A", aData);
    sourceMaker.AddColumn("B", bData);
    var source = sourceMaker.MakeTable(ctx.Client.Manager);

    var result = source.SelectDistinct("A");
    _output.WriteLine(result.ToString(true));

    var expectedAData = new[] { "apple", "orange", "plum", "grape" };

    var tc = new TableComparer();
    tc.AddColumn("A", expectedAData);
    tc.AssertEqualTo(result);
  }
}
