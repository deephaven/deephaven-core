namespace Deephaven.DeephavenClient.ExcelAddIn.Util;

internal class ActionAsDisposable : IDisposable {
  private Action? _action;

  public ActionAsDisposable(Action action) => _action = action;

  public void Dispose() {
    var temp = _action;
    if (temp == null) {
      return;
    }

    _action = null;
    temp();
  }
}
