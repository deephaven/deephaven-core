using System.Diagnostics;

namespace Deephaven.ExcelAddIn.Util;

public class WorkerThread {
  public static WorkerThread Create() {
    var result = new WorkerThread();
    var t = new Thread(result.Doit) { IsBackground = true };
    result._thisThread = t;
    t.Start();
    return result;
  }

  private readonly object _sync = new();
  private readonly Queue<Action> _queue = new();
  private Thread? _thisThread;

  private WorkerThread() {
  }

  public void Invoke(Action action) {
    if (!InvokeIfRequired(action)) {
      action();
    }
  }

  public bool InvokeIfRequired(Action action) {
    if (ReferenceEquals(Thread.CurrentThread, _thisThread)) {
      // Appending to thread queue was not required. Return false.
      return false;
    }

    lock (_sync) {
      _queue.Enqueue(action);
      if (_queue.Count == 1) {
        // Only need to pulse on transition from 0 to 1, because the
        // Doit method only Waits if the queue is empty.
        Monitor.PulseAll(_sync);
      }
    }

    // Appending to thread queue was required.
    return true;
  }

  private void Doit() {
    while (true) {
      Action action;
      lock (_sync) {
        while (_queue.Count == 0) {
          Monitor.Wait(_sync);
        }

        action = _queue.Dequeue();
      }

      try {
        action();
      } catch (Exception ex) {
        Debug.WriteLine($"Swallowing exception {ex}");
      }
    }
  }
}
