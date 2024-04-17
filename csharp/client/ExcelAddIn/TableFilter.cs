using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Deephaven.DeephavenClient.ExcelAddIn;

internal class TableFilter {
  public static readonly TableFilter Default = new ();
}

// Can't decide where to put this
internal class ActionDisposable : IDisposable {
  private Action? _action;

  public ActionDisposable(Action action) => _action = action;

  public void Dispose() {
    Console.WriteLine("disposing why");
    var temp = _action;
    if (temp == null) {
      return;
    }

    Console.WriteLine("disposing why");
    _action = null;
    temp();
  }
}
