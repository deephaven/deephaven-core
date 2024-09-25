using System.Diagnostics.CodeAnalysis;
using System.Windows.Forms;

namespace Deephaven.ExcelAddIn.Util;

public sealed class StatusOr<T> {
  private readonly string? _status;
  private readonly T? _value;

  public static StatusOr<T> OfStatus(string status) {
    return new StatusOr<T>(status, default);
  }

  public static StatusOr<T> OfValue(T value) {
    return new StatusOr<T>(null, value);
  }

  private StatusOr(string? status, T? value) {
    _status = status;
    _value = value;
  }

  public bool GetValueOrStatus(
    [NotNullWhen(true)]out T? value,
    [NotNullWhen(false)]out string? status) {
    status = _status;
    value = _value;
    return value != null;
  }

  public U AcceptVisitor<U>(Func<T, U> onValue, Func<string, U> onStatus) {
    return _value != null ? onValue(_value) : onStatus(_status!);
  }
}

public static class ObserverStatusOr_Extensions {
  public static void SendStatus<T>(this IObserver<StatusOr<T>> observer, string message) {
    var so = StatusOr<T>.OfStatus(message);
    observer.OnNext(so);
  }

  public static void SendValue<T>(this IObserver<StatusOr<T>> observer, T value) {
    var so = StatusOr<T>.OfValue(value);
    observer.OnNext(so);
  }

  public static void SetAndSendStatus<T>(this IObserver<StatusOr<T>> observer, ref StatusOr<T> sor,
    string message) {
    SetAndSend(observer, ref sor, StatusOr<T>.OfStatus(message));
  }

  public static void SetAndSendValue<T>(this IObserver<StatusOr<T>> observer, ref StatusOr<T> sor,
    T value) {
    SetAndSend(observer, ref sor, StatusOr<T>.OfValue(value));
  }

  public static void SetAndSend<T>(this IObserver<StatusOr<T>> observer, ref StatusOr<T> sor,
    StatusOr<T> newSor) {
    sor = newSor;
    observer.OnNext(newSor);
  }
}
