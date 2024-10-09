using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Deephaven.DeephavenClient.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Util;

internal static class ObservableConverter {
  public static ObservableConverter<TFrom, TTo> Create<TFrom, TTo>(
    Func<TFrom, TTo> converter, WorkerThread workerThread) {
    return new ObservableConverter<TFrom, TTo>(converter, workerThread);
  }
}

internal class ObservableConverter<TFrom, TTo>(Func<TFrom, TTo> converter, WorkerThread workerThread) :
  IObserver<TFrom>, IObservable<TTo> {
  private readonly ObserverContainer<TTo> _observers = new();

  public void OnNext(TFrom value) {
    var converted = converter(value);
    _observers.OnNext(converted);
  }

  public void OnCompleted() {
    _observers.OnCompleted();
  }

  public void OnError(Exception error) {
    _observers.OnError(error);
  }

  public IDisposable Subscribe(IObserver<TTo> observer) {
    workerThread.EnqueueOrRun(() => _observers.Add(observer, out _));

    return ActionAsDisposable.Create(() => {
      workerThread.EnqueueOrRun(() => {
        _observers.Remove(observer, out _);
      });
    });
  }
}
