using Deephaven.DeephavenClient.Interop;
using Deephaven.DeephavenClient;
using Xunit.Abstractions;
using System;

namespace Deephaven.DhClientTests;

public class BasicTests {
  private readonly ITestOutputHelper _output;

  public BasicTests(ITestOutputHelper output) {
    _output = output;
  }

  [Fact]
  public void TestClosePlaysNiceWithDispose() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;
    using var updated = table.Update("QQQ = i");
    ctx.Client.Close();
  }
}
