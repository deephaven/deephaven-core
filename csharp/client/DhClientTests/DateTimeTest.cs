using Deephaven.DeephavenClient.Interop;
using Deephaven.DeephavenClient;
using Xunit.Abstractions;

namespace Deephaven.DhClientTests;

public class DateTimeTest {
  private readonly ITestOutputHelper _output;

  public DateTimeTest(ITestOutputHelper output) {
    _output = output;
  }

  [Fact]
  public void TestDhDateTimeConstructor() {
    var dt0 = new DhDateTime(2001, 3, 1, 12, 34, 56);
    Assert.Equal(983450096000000000, dt0.Nanos);
    var dt1 = new DhDateTime(2001, 3, 1, 12, 34, 56, 987000000);
    Assert.Equal(983450096987000000, dt1.Nanos);
    var dt2 = new DhDateTime(2001, 3, 1, 12, 34, 56, 987654000);
    Assert.Equal(983450096987654000, dt2.Nanos);
    var dt3 = new DhDateTime(2001, 3, 1, 12, 34, 56, 987654321);
    Assert.Equal(983450096987654321, dt3.Nanos);
  }

  // TODO(kosak): DhDateTime.Parse (including parsing full nanosecond resolution)
  // and DhDateTime.Format
}
