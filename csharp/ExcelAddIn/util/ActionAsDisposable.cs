namespace Deephaven.DeephavenClient.ExcelAddIn.Util;

internal class ActionAsDisposable : IDisposable {
  public static IDisposable Create(Action action) {
    return new ActionAsDisposable(action);
  }

  private Action? _action;

  private ActionAsDisposable(Action action) => _action = action;

  public void Dispose() {
    var temp = _action;
    if (temp == null) {
      return;
    }

    _action = null;
    temp();
  }
}
