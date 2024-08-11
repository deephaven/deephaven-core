using ExcelDna.Integration;

namespace Deephaven.DeephavenClient.ExcelAddIn.ExcelDna;

/// <summary>
/// This interface supports managing the mutation (adding and removing) from a collection
/// of IExcelObservers.
/// </summary>
public interface IObserverCollection {
  /// <summary>
  /// Adds an observer to the collection.
  /// </summary>
  /// <param name="observer">The observer</param>
  /// <param name="isFirst">True iff this was the first observer that was added.</param>
  void Add(IExcelObserver observer, out bool isFirst);
  /// <summary>
  /// Removes an observer from the collection.
  /// </summary>
  /// <param name="observer">The observer</param>
  /// <param name="wasLast">True iff this was the final observer that was removed
  /// (leaving the collection empty)</param>
  void Remove(IExcelObserver observer, out bool wasLast);
}

/// <summary>
/// This interface supports the operations for communicating status to a collection
/// of IExcelObservers.
/// </summary>
public interface IDataListener {
  /// <summary>
  /// Transmits a status message to the observers.
  /// </summary>
  public void OnStatus(string message) {
    var matrix = new object[1, 1];
    matrix[0, 0] = message;
    OnNext(matrix);
  }

  /// <summary>
  /// Transmits an exception to the observers.
  /// </summary>
  public void OnError(Exception error) {
    OnStatus(error.Message);
  }

  /// <summary>
  /// Transmits a rectangular array of data to the observers.
  /// </summary>
  /// <param name="data"></param>
  public void OnNext(object?[,] data);
}

/// <summary>
/// This class implements both the above interfaces.
/// </summary>
public sealed class ObserverContainer : IObserverCollection, IDataListener {
  private readonly object _sync = new();
  private readonly HashSet<IExcelObserver> _observers = new();

  public IObserverCollection GetObserverCollection() {
    return this;
  }

  public IDataListener GetDataListener() {
    return this;
  }

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
