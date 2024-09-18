namespace Deephaven.ExcelAddIn.Models;

public record AddOrRemove<T>(bool IsAdd, T Value) {
  public static AddOrRemove<T> OfAdd(T value) {
    return new AddOrRemove<T>(true, value);
  }

  public static AddOrRemove<T> OfRemove(T value) {
    return new AddOrRemove<T>(false, value);
  }
}

public record EndpointId(string Id) {
  public override string ToString() => Id;
}

public record PersistentQueryId(string Id);
