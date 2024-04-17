using Deephaven.DeephavenClient.ExcelAddIn;

namespace ExcelAddIn {
  public partial class Form1 : Form {
    private readonly Action<Form, string> _onConnect;

    public Form1(ConnectionDialogViewModel vm, Action<Form, string> onConnect) {
      _onConnect = onConnect;
      InitializeComponent();
      this.connectionStringText.DataBindings.Add("Text", vm, "ConnectionString");
    }

    private void Form1_Load(object sender, EventArgs e) {

    }

    private void connectButton_Click(object sender, EventArgs e) {
      _onConnect(this, this.connectionStringText.Text.Trim());
    }
  }
}
