using System;
using Deephaven.DeephavenClient;
using Deephaven.DeephavenClient.Utility;

namespace Deephaven.DhClientTests;

public sealed class CommonContextForTests : IDisposable {
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
    var host = GlobalEnvironmentForTests.GetEnv("DH_HOST", "10.0.4.106");
    var port = GlobalEnvironmentForTests.GetEnv("DH_PORT", "10000");
    var connectionString = $"{host}:{port}";
    var client = Client.Connect(connectionString, clientOptions);
    return client;
  }
}

// TODO(kosak): put this somewhere, and implement for real
class GlobalEnvironmentForTests {
  public static string GetEnv(string environmentVariable, string defaultValue) {
    return defaultValue;
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
  public string[] ImportDate = {
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
  };

  public string[] Ticker = {
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
  };

  public double[] Open = {
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
  };

  public double[] Close = {
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
  };

  public long[] Volume = {
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
  };
}
