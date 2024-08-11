using Deephaven.DeephavenClient.ExcelAddIn.Operations;
using Deephaven.DeephavenClient.ExcelAddIn.Util;
using ExcelDna.Integration;

namespace Deephaven.DeephavenClient.ExcelAddIn.ExcelDna;

/// <summary>
/// TODO(kosak): make this make sense.
/// 
/// This class implements IExcelObservable, which itself is simply an implementation
/// of the .NET Observable pattern. The basic idea is that there might be multiple
/// IExcelObservers who want to observe us. Initially, the state is quiescent:
/// no one has observed us yet, and so we have not started our attempt to get
/// data from Deephaven either.
///
/// On the first call to Subscribe, we add the observer to our collection of observers,
/// and we also register the operation with the OperationManager. This will in turn
/// indirectly invoke IOperation.Start(), which will begin the specified interaction
/// with Deephaven (for example, fetching or subscribing to a Deephaven table).
///
/// Subsequent calls to Subscribe just add the observer to the collection.
///
/// The inverse happens when subscriptions are disposed. When subscriptions (other
/// than the last) are disposed, they are simply removed from the collection.
/// When the final subscription is disposed, the table operation is unregistered
/// from the OperationManager. This in turn will invoke IOperation.Stop() which will
/// release whatever Deephaven resources (releasing the table handle and/or unsubscribing
/// from the ticking table).
/// </summary>
internal sealed class DeephavenExcelObservable : IExcelObservable {
  private readonly OperationManager _operationManager;
  private readonly IOperation _tableOperation;
  private readonly IObserverCollection _collection;

  public DeephavenExcelObservable(OperationManager tableOperationManager, IOperation tableOperation,
    IObserverCollection collection) {
    _operationManager = tableOperationManager;
    _tableOperation = tableOperation;
    _collection = collection;
  }

  public IDisposable Subscribe(IExcelObserver observer) {
    _collection.Add(observer, out var isFirst);

    if (isFirst) {
      _operationManager.Register(_tableOperation);
    }

    return new ActionDisposable(() => RemoveObserver(observer));
  }

  private void RemoveObserver(IExcelObserver observer) {
    _collection.Remove(observer, out var wasLast);
    if (wasLast) {
      _operationManager.Unregister(_tableOperation);
    }
  }
}
