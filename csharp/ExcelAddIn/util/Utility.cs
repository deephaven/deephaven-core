
namespace Deephaven.ExcelAddIn.Util;

internal static class Utility {
  public static T Exchange<T>(ref T item, T newValue) {
    var result = item;
    item = newValue;
    return result;
  }
}

public class Unit {
  public static readonly Unit Instance = new Unit();

  private Unit() {
  }
}
