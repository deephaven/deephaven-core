namespace Deephaven.ExcelAddIn.Util;

internal class SimpleAtomicReference<T>(T value) {
  private readonly object _sync = new();
  private T _value = value;

  public T Value {
    get {
      lock (_sync) {
        return _value;
      }
    }
    set {
      lock (_sync) {
        _value = value;
      }
    }
  }
}
