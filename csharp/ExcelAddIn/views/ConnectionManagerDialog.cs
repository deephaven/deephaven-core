using Deephaven.ExcelAddIn.Viewmodels;

namespace Deephaven.ExcelAddIn.Views;

using SelectedRowsAction = Action<ConnectionManagerDialogRow[]>;

public partial class ConnectionManagerDialog : Form {
  private const string IsDefaultColumnName = "IsDefault";
  private readonly Action _onNewButtonClicked;
  private readonly SelectedRowsAction _onDeleteButtonClicked;
  private readonly SelectedRowsAction _onReconnectButtonClicked;
  private readonly SelectedRowsAction _onMakeDefaultButtonClicked;
  private readonly SelectedRowsAction _onEditButtonClicked;
  private readonly BindingSource _bindingSource = new();

  public ConnectionManagerDialog(Action onNewButtonClicked,
    SelectedRowsAction onDeleteButtonClicked,
    SelectedRowsAction onReconnectButtonClicked,
    SelectedRowsAction onMakeDefaultButtonClicked,
    SelectedRowsAction onEditButtonClicked) {
    _onNewButtonClicked = onNewButtonClicked;
    _onDeleteButtonClicked = onDeleteButtonClicked;
    _onReconnectButtonClicked = onReconnectButtonClicked;
    _onMakeDefaultButtonClicked = onMakeDefaultButtonClicked;
    _onEditButtonClicked = onEditButtonClicked;

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
    _onNewButtonClicked();
  }

  private void reconnectButton_Click(object sender, EventArgs e) {
    var selections = GetSelectedRows();
    _onReconnectButtonClicked(selections);
  }

  private void editButton_Click(object sender, EventArgs e) {
    var selections = GetSelectedRows();
    _onEditButtonClicked(selections);
  }

  private void deleteButton_Click(object sender, EventArgs e) {
    var selections = GetSelectedRows();
    _onDeleteButtonClicked(selections);
  }

  private void makeDefaultButton_Click(object sender, EventArgs e) {
    var selections = GetSelectedRows();
    _onMakeDefaultButtonClicked(selections);
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
