//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;
using Xunit.Abstractions;

namespace Deephaven.Dh_NetClientTests;

public class ValidationTest(ITestOutputHelper output) {
  [Theory]
  [InlineData("X = 3)")]  // Syntax error
  [InlineData("S = `hello`", "T = java.util.regex.Pattern.quote(S)")]  // Pattern.quote not on whitelist
  [InlineData("X = Math.min(3, 4)")]  // Math.min not on whitelist
  public void BadSelect(params string[] selections) {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;
    using var staticTable = thm.EmptyTable(10);
    Assert.Throws<AggregateException>(() => {
      using var temp = staticTable.Select(selections);
    });
  }


  [Theory]
  [InlineData("X = 3")]
  [InlineData("S = `hello`", "T = S.length()")]  // instance methods of String ok
  [InlineData("X = min(3, 4)")]  // builtin from GroovyStaticImports
  [InlineData("X = isFinite(3)")]  // another builtin from GroovyStaticImports
  public void GoodSelect(params string[] selections) {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;
    using var staticTable = thm.EmptyTable(10)
      .Select(selections);
  }

  [Theory]
  [InlineData("X > 3)")]  // syntax error
  [InlineData("S = java.util.regex.Pattern.quote(S)")]  // Pattern.quote not on whitelist
  [InlineData("X = Math.min(3, 4)")]  // Math.min not on whitelist
  public void BadWhere(string condition) {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;
    using var staticTable = thm.EmptyTable(10)
      .Update("X = 12", "S = `hello`");

    Assert.Throws<AggregateException>(() => {
      using var temp = staticTable.Where(condition);
    });
  }

  [Theory]
  [InlineData("X = 3")]
  [InlineData("S = `hello`")]
  [InlineData("S.length() = 17")] // instance methods of String ok
  [InlineData("X = min(3, 4)")] // "builtin" from GroovyStaticImports
  [InlineData("X = isFinite(3)")] // another builtin from GroovyStaticImports
  [InlineData("X in 3, 4, 5")]
  public void GoodWhere(string condition) {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;
    using var staticTable = thm.EmptyTable(10)
      .Update("X = 12", "S = `hello`")
      .Where(condition);
  }
}
