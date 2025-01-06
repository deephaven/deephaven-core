
using System.Diagnostics;

namespace Deephaven.ExcelAddIn.Util;

internal static class Utility {
  public static T Exchange<T>(ref T item, T newValue) {
    var result = item;
    item = newValue;
    return result;
  }

  public static void RunInBackground(Action a) {
    void Doit() {
      try {
        a();
      } catch (Exception e) {
        Debug.WriteLine($"Ignoring exception {e}");
      }
    }
    new Thread(Doit) { IsBackground = true }.Start();
  }
}

public class Unit {
  public static readonly Unit Instance = new ();

  private Unit() {
  }
}

public class ValueHolder<T> where T : class {
  private T? _value = null;

  public T Value {
    get {
      if (_value == null) {
        throw new Exception("Value is unset");
      }

      return _value;
    }
    set => _value = value;
  }
}
