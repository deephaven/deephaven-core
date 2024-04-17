using ExcelDna.Integration;

namespace Deephaven.DeephavenClient.ExcelAddIn;

internal sealed class DeephavenHandler : IExcelObservable {
  private readonly TableOperationManager _tableOperationManager;
  private readonly IDeephavenTableOperation _tableOperation;
  private readonly ObserverContainer _observerContainer;

  public DeephavenHandler(TableOperationManager tableOperationManager, IDeephavenTableOperation tableOperation,
    ObserverContainer observerContainer) {
    _tableOperationManager = tableOperationManager;
    _tableOperation = tableOperation;
    _observerContainer = observerContainer;
  }

  public IDisposable Subscribe(IExcelObserver observer) {
    _observerContainer.Add(observer, out var isFirst);

    if (isFirst) {
      _tableOperationManager.Register(_tableOperation);
    }

    return new ActionDisposable(() => RemoveObserver(observer));
  }

  private void RemoveObserver(IExcelObserver observer) {
    _observerContainer.Remove(observer, out var wasLast);
    if (wasLast) {
      _tableOperationManager.Unregister(_tableOperation);
    }
  }
}

public class ObserverContainer {
  private readonly object _sync = new();
  private readonly HashSet<IExcelObserver> _observers = new();

  public void Add(IExcelObserver observer, out bool isFirst) {
    lock (_sync) {
      isFirst = _observers.Count == 0;
      _observers.Add(observer);
    }
  }

  public void Remove(IExcelObserver observer, out bool wasLast) {
    lock (_sync) {
      _observers.Remove(observer);
      wasLast = _observers.Count == 0;
    }
  }

  public void OnStatus(string message) {
    var matrix = new object[1, 1];
    matrix[0, 0] = message;
    OnNext(matrix);
  }

  public void OnError(Exception error) {
    OnStatus(error.Message);
  }

  public void OnNext(object?[,] result) {
    IExcelObserver[] observers;
    lock (_sync) {
      observers = _observers.ToArray();
    }

    foreach (var observer in observers) {
      observer.OnNext(result);
    }
  }
}

public interface IDeephavenTableOperation {
  void Start(ClientOrStatus clientOrStatus);
  void Stop();
}
