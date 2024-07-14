namespace Deephaven.DeephavenClient.ExcelAddIn.Util;

internal class ActionDisposable : IDisposable {
  private Action? _action;

  public ActionDisposable(Action action) => _action = action;

  public void Dispose() {
    var temp = _action;
    if (temp == null) {
      return;
    }

    _action = null;
    temp();
  }
}
