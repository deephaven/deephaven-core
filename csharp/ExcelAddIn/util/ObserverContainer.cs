namespace Deephaven.ExcelAddIn.Util;

public sealed class ObserverContainer<T> : IObserver<T> {
  private readonly object _sync = new();
  private readonly HashSet<IObserver<T>> _observers = new();

  public int Count {
    get {
      lock (_sync) {
        return _observers.Count;
      }
    }
  }

  public void Add(IObserver<T> observer, out bool isFirst) {
    lock (_sync) {
      isFirst = _observers.Count == 0;
      _observers.Add(observer);
    }
  }

  public void Remove(IObserver<T> observer, out bool wasLast) {
    lock (_sync) {
      var removed = _observers.Remove(observer);
      wasLast = removed && _observers.Count == 0;
    }
  }

  public void OnNext(T result) {
    foreach (var observer in SafeCopyObservers()) {
      observer.OnNext(result);
    }
  }

  public void OnError(Exception ex) {
    foreach (var observer in SafeCopyObservers()) {
      observer.OnError(ex);
    }
  }

  public void OnCompleted() {
    foreach (var observer in SafeCopyObservers()) {
      observer.OnCompleted();
    }
  }

  private IObserver<T>[] SafeCopyObservers() {
    lock (_sync) {
      return _observers.ToArray();
    }
  }
}
