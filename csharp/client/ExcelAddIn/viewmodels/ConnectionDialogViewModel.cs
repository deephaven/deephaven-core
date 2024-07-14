namespace Deephaven.DeephavenClient.ExcelAddIn.ViewModels;

public class ConnectionDialogViewModel {
  private string _connectionString = GlobalConstants.DefaultConnectionString;

  public event EventHandler? ConnectionStringChanged;

  public string ConnectionString {
    get => _connectionString;
    set {
      if (_connectionString == value) {
        return;
      }
      _connectionString = value;
      OnConnectionStringChanged();
    }
  }

  private void OnConnectionStringChanged() {
    ConnectionStringChanged?.Invoke(this, EventArgs.Empty);
  }
}
