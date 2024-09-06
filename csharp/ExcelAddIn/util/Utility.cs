
namespace Deephaven.ExcelAddIn.Util;

internal static class Utility {
  public static T Exchange<T>(ref T item, T newValue) {
    var result = item;
    item = newValue;
    return result;
  }

  public static void RunInBackground(Action a) {
    new Thread(() => a()) { IsBackground = true }.Start();
  }

  public static void IgnoreExceptions(Action action) {
    try {
      action();
    } catch {
      // Ignore errors
    }
  }
}

public class Unit {
  public static readonly Unit Instance = new Unit();

  private Unit() {
  }
}
