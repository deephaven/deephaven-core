//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public sealed class CommonContextForTests : IDisposable {
  /// <summary>
  /// We provide two options for setting the server host and port when running unit tests.
  /// 1. Look for the environment variables DH_HOST and DH_PORT
  /// 2. If not found, use DefaultDhHost and DefaultDhPort
  /// The Visual Studio test runner will read environment variable settings from the .runsettings file
  /// in the project directory. However, note that the Resharper test runner does not seem to honor
  /// .runsettings.
  /// </summary>
  private const string? DefaultDhHost = null;

  private const string? DefaultDhPort = null;

  public readonly Client Client;
  public readonly TableHandle TestTable;
  public readonly ColumnNamesForTests ColumnNames;
  public readonly ColumnDataForTests ColumnData;

  public static CommonContextForTests Create(ClientOptions options) {
    var client = CreateClient(options);
    var manager = client.Manager;

    var cn = new ColumnNamesForTests();
    var cd = new ColumnDataForTests();

    var maker = new TableMaker();
    maker.AddColumn(cn.ImportDate, cd.ImportDate);
    maker.AddColumn(cn.Ticker, cd.Ticker);
    maker.AddColumn(cn.Open, cd.Open);
    maker.AddColumn(cn.Close, cd.Close);
    maker.AddColumn(cn.Volume, cd.Volume);

    var testTable = maker.MakeTable(manager);
    return new CommonContextForTests(client, testTable, cn, cd);
  }

  private CommonContextForTests(Client client, TableHandle testTable,
    ColumnNamesForTests cn, ColumnDataForTests cd) {
    Client = client;
    TestTable = testTable;
    ColumnNames = cn;
    ColumnData = cd;
  }

  public void Dispose() {
    TestTable.Dispose();
    Client.Dispose();
  }

  private static Client CreateClient(ClientOptions clientOptions) {
    var missing = new List<string>();
    var host = TryGetEnv("DH_HOST", DefaultDhHost, missing);
    var port = TryGetEnv("DH_PORT", DefaultDhPort, missing);

    if (missing.Count != 0) {
      throw new Exception($"The following environment variables were not found: {string.Join(", ", missing)}.\n" +
        "Please set them in your environment.\n" +
        "Sample values:\n" +
        "  DH_HOST=10.0.4.109\n" +
        "  DH_PORT=10000\n" +
        "If using the Visual Studio test runner you can edit the .runsettings file in the project directory.\n" +
        "However please note that if you are using the *ReSharper* test runner it will not honor .runsettings\n" +
        $"Otherwise you can edit {nameof(CommonContextForTests)}.{nameof(DefaultDhHost)} and " +
        $"{nameof(CommonContextForTests)}.{nameof(DefaultDhPort)}");
    }

    var connectionString = $"{host}:{port}";
    var client = Client.Connect(connectionString, clientOptions);
    return client;
  }

  private static string? TryGetEnv(string envName, string? defaultValue, List<string> failures) {
    var enVal = Environment.GetEnvironmentVariable(envName);
    if (enVal != null) {
      return enVal;
    }
    if (defaultValue != null) {
      return defaultValue;
    }

    failures.Add(envName);
    return null;
  }
}

public class ColumnNamesForTests {
  public string ImportDate = "ImportDate";
  public string Ticker = "Ticker";
  public string Open = "Open";
  public string Close = "Close";
  public string Volume = "Volume";
}

public class ColumnDataForTests {
  public string[] ImportDate = [
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-02"
  ];

  public string[] Ticker = [
    "XRX",
    "XRX",
    "XYZZY",
    "IBM",
    "GME",
    "AAPL",
    "AAPL",
    "AAPL",
    "ZNGA",
    "ZNGA",
    "T"
  ];

  public double[] Open = [
    83.1,
    50.5,
    92.3,
    40.1,
    681.43,
    22.1,
    26.8,
    31.5,
    541.2,
    685.3,
    18.8
  ];

  public double[] Close = [
    88.2,
    53.8,
    88.5,
    38.7,
    453,
    23.5,
    24.2,
    26.7,
    538.2,
    544.9,
    13.4
  ];

  public long[] Volume = [
    345000,
    87000,
    6060842,
    138000,
    138000000,
    100000,
    250000,
    19000,
    46123,
    48300,
    1500
  ];
}
