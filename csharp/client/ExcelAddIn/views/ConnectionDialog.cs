using Deephaven.DeephavenClient.ExcelAddIn;
using Deephaven.DeephavenClient.ExcelAddIn.ViewModels;

namespace Deephaven.DeephavenClient.ExcelAddIn.Views {
  public partial class ConnectionDialog : Form {
    private readonly Action<Form, string> _onConnect;

    public ConnectionDialog(ConnectionDialogViewModel vm, Action<Form, string> onConnect) {
      _onConnect = onConnect;
      InitializeComponent();
      this.connectionStringText.DataBindings.Add("Text", vm, "ConnectionString");
    }

    private void connectButton_Click(object sender, EventArgs e) {
      _onConnect(this, this.connectionStringText.Text.Trim());
    }
  }
}
