using ExcelDna.Integration;

namespace Deephaven.DeephavenClient.ExcelAddIn.ExcelDna;

internal static class InterpretOptional {
  public static bool TryInterpretBool(object value, bool defaultValue, out bool result) {
    result = defaultValue;
    if (value is ExcelMissing) {
      return true;
    }
    if (value is bool b) {
      result = b;
      return true;
    }

    return false;
  }
}
