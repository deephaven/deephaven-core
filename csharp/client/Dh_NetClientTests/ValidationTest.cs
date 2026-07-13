//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class ValidationTest {
  [Test]
  [Arguments("X = 3)")] // Syntax error
  [Arguments("S = `hello`", "T = java.util.regex.Pattern.quote(S)")] // Pattern.quote not on whitelist
  [Arguments("X = Math.min(3, 4)")] // Math.min not on whitelist
  public async Task BadSelect(params string[] selections) {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;
    using var staticTable = thm.EmptyTable(10);
    await Assert.That(() => {
      using var _ = staticTable.Select(selections);
    }).Throws<AggregateException>();
  }


  [Test]
  [Arguments("X = 3")]
  [Arguments("S = `hello`", "T = S.length()")]  // instance methods of String ok
  [Arguments("X = min(3, 4)")]  // builtin from GroovyStaticImports
  [Arguments("X = isFinite(3)")]  // another builtin from GroovyStaticImports
  public void GoodSelect(params string[] selections) {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;
    using var staticTable = thm.EmptyTable(10)
      .Select(selections);
  }

  [Test]
  [Arguments("X > 3)")] // syntax error
  [Arguments("S = java.util.regex.Pattern.quote(S)")] // Pattern.quote not on whitelist
  [Arguments("X = Math.min(3, 4)")] // Math.min not on whitelist
  public async Task BadWhere(string condition) {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;
    using var staticTable = thm.EmptyTable(10)
      .Update("X = 12", "S = `hello`");

    await Assert.That(() => {
      using var _ = staticTable.Where(condition);
    }).Throws<AggregateException>();
  }

  [Test]
  [Arguments("X = 3")]
  [Arguments("S = `hello`")]
  [Arguments("S.length() = 17")] // instance methods of String ok
  [Arguments("X = min(3, 4)")] // "builtin" from GroovyStaticImports
  [Arguments("X = isFinite(3)")] // another builtin from GroovyStaticImports
  [Arguments("X in 3, 4, 5")]
  public void GoodWhere(string condition) {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;
    using var staticTable = thm.EmptyTable(10)
      .Update("X = 12", "S = `hello`")
      .Where(condition);
  }
}