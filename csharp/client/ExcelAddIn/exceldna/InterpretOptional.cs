using ExcelDna.Integration;

namespace Deephaven.DeephavenClient.ExcelAddIn.ExcelDna;

internal static class InterpretOptional {
  public static bool TryInterpretAs<T>(object value, T defaultValue, out T result) {
    result = defaultValue;
    if (value is ExcelMissing) {
      return true;
    }
    if (value is T tValue) {
      result = tValue;
      return true;
    }

    return false;
  }
}
