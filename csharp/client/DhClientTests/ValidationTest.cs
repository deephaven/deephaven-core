using Deephaven.DeephavenClient;
using Deephaven.DeephavenClient.Utility;
using System;
using Xunit.Abstractions;

namespace Deephaven.DhClientTests;

public class ValidationTest {
  private readonly ITestOutputHelper _output;

  public ValidationTest(ITestOutputHelper output) {
    _output = output;
  }

  [Fact]
  public void Select() {
    var badSelects = new[] {
      new[] { "X = 3)" },
      new[] { "S = `hello`", "T = java.util.regex.Pattern.quote(S)" }, // Pattern.quote not on whitelist
      new[] { "X = Math.min(3, 4)" } // Math.min not on whitelist
    };
    var goodSelects = new[] {
      new[] { "X = 3" },
      new[] { "S = `hello`", "T = S.length()" }, // instance methods of String ok
      new[] { "X = min(3, 4)" }, // "builtin" from GroovyStaticImports
      new[] { "X = isFinite(3)" }, // another builtin from GroovyStaticImports
    };

    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;
    using var staticTable = thm.EmptyTable(10)
      .Update("X = 12", "S = `hello`");
    TestSelectsHelper("static Table", staticTable, badSelects, goodSelects);
  }

  [Fact]
  public void Where() {
    var badWheres = new[] {
      "X > 3)", // syntax error
      "S = new String(`hello`)", // new not allowed
      "S = java.util.regex.Pattern.quote(S)", // Pattern.quote not on whitelist
      "X = Math.min(3, 4)" // Math.min not on whitelist
    };

    var goodWheres = new[] {
      "X = 3",
      "S = `hello`",
      "S.length() = 17", // instance methods of String ok
      "X = min(3, 4)", // "builtin" from GroovyStaticImports
      "X = isFinite(3)", // another builtin from GroovyStaticImports
      "X in 3, 4, 5",
    };

    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;
    using var staticTable = thm.EmptyTable(10)
      .Update("X = 12", "S = `hello`");
    TestWheresHelper("static Table", staticTable, badWheres, goodWheres);
  }

  private void TestWheresHelper(string what, TableHandle table,
    IEnumerable<string> badWheres, IEnumerable<string> goodWheres) {
    foreach (var bw in badWheres) {
      try {
        _output.WriteLine($"Trying {what} {bw}");
        using var dummy = table.Where(bw);
      } catch (Exception e) {
        _output.WriteLine($"{what}: {bw}: Failed *as expected* with: {e.Message}");
        continue;
      }

      throw new Exception($"{what}: {bw}: Expected to fail, but succeeded");
    }

    foreach (var gw in goodWheres) {
      using var dummy = table.Where(gw);
      _output.WriteLine($"{what}: {gw}: Succeeded as expected");
    }
  }

  private void TestSelectsHelper(string what, TableHandle table,
    IEnumerable<string[]> badSelects, IEnumerable<string[]> goodSelects) {
    foreach (var bs in badSelects) {
      var printable = string.Join(", ", bs);
      try {
        using var dummy = table.Select(bs);
      } catch (Exception e) {
        _output.WriteLine($"{what}: {printable}: Failed as expected with: {e.Message}");
        continue;
      }

      throw new Exception($"{what}: {printable}: Expected to fail, but succeeded");
    }

    foreach (var gs in goodSelects) {
      var printable = string.Join(", ", gs);
      using var dummy = table.Select(gs);
      _output.WriteLine($"{what}: {printable}: Succeeded as expected");
    }
  }
}
