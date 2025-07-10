using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class UtilityTest {
  [Fact]
  public void TestConvertToBase52() {
    var dest = new char[4];
    Assert.True(Utility.TryConvertToBase52(0, dest));
    Assert.Equal("AAAA", new string(dest));

    Assert.True(Utility.TryConvertToBase52(25, dest));
    Assert.Equal("AAAZ", new string(dest));

    Assert.True(Utility.TryConvertToBase52(51, dest));
    Assert.Equal("AAAz", new string(dest));

    Assert.True(Utility.TryConvertToBase52(52, dest));
    Assert.Equal("AABA", new string(dest));

    Assert.True(Utility.TryConvertToBase52(3 * 52 * 52 * 52 + 33 * 52 * 52 + 47 * 52 + 39, dest));
    Assert.Equal("Dhvn", new string(dest));

    Assert.False(Utility.TryConvertToBase52(-1, dest));
    Assert.False(Utility.TryConvertToBase52(52 * 52 * 52 * 52, dest));
  }
}
