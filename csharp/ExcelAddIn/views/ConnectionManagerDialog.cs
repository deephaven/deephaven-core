using Deephaven.ExcelAddIn.Viewmodels;

namespace Deephaven.ExcelAddIn.Views;

using SelectedRowsAction = Action<ConnectionManagerDialogRow[]>;

public partial class ConnectionManagerDialog : Form {
  public event Action? OnNewButtonClicked;
  public event SelectedRowsAction? OnDeleteButtonClicked;
  public event SelectedRowsAction? OnReconnectButtonClicked;
  public event SelectedRowsAction? OnMakeDefaultButtonClicked;
  public event SelectedRowsAction? OnEditButtonClicked;

  private readonly BindingSource _bindingSource = new();

  public ConnectionManagerDialog() {
    InitializeComponent();

    _bindingSource.DataSource = typeof(ConnectionManagerDialogRow);
    dataGridView1.DataSource = _bindingSource;
  }

  public void AddRow(ConnectionManagerDialogRow row) {
    if (InvokeRequired) {
      Invoke(() => AddRow(row));
      return;
    }
    _bindingSource.Add(row);
    dataGridView1.ClearSelection();
  }

  public void RemoveRow(ConnectionManagerDialogRow row) {
    if (InvokeRequired) {
      Invoke(() => RemoveRow(row));
      return;
    }
    _bindingSource.Remove(row);
  }

  private void newButton_Click(object sender, EventArgs e) {
    OnNewButtonClicked?.Invoke();
  }

  private void reconnectButton_Click(object sender, EventArgs e) {
    var selections = GetSelectedRows();
    OnReconnectButtonClicked?.Invoke(selections);
  }

  private void editButton_Click(object sender, EventArgs e) {
    var selections = GetSelectedRows();
    OnEditButtonClicked?.Invoke(selections);
  }

  private void deleteButton_Click(object sender, EventArgs e) {
    var selections = GetSelectedRows();
    OnDeleteButtonClicked?.Invoke(selections);
  }

  private void makeDefaultButton_Click(object sender, EventArgs e) {
    var selections = GetSelectedRows();
    OnMakeDefaultButtonClicked?.Invoke(selections);
  }

  private ConnectionManagerDialogRow[] GetSelectedRows() {
    var result = new List<ConnectionManagerDialogRow>();
    var sr = dataGridView1.SelectedRows;
    var count = sr.Count;
    for (var i = 0; i != count; ++i) {
      result.Add((ConnectionManagerDialogRow)sr[i].DataBoundItem);
    }
    return result.ToArray();
  }
}
