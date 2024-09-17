using Deephaven.DeephavenClient;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Providers;

/// <summary>
/// Common interface for TableProvider, FilteredTableProvider, and DefaultEndpointTableProvider
/// </summary>
public interface ITableProvider : IObservable<StatusOr<TableHandle>> {
  void Init();
}
