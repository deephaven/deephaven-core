using Deephaven.ExcelAddIn.ViewModels;

namespace ExcelAddIn.views {
  public partial class CredentialsDialog : Form {
    private readonly Action _onSetCredentialsButtonClicked;
    private readonly Action _onTestCredentialsButtonClicked;

    public CredentialsDialog(CredentialsDialogViewModel vm, Action onSetCredentialsButtonClicked,
      Action onTestCredentialsButtonClicked) {
      _onSetCredentialsButtonClicked = onSetCredentialsButtonClicked;
      _onTestCredentialsButtonClicked = onTestCredentialsButtonClicked;

      InitializeComponent();
      // Need to fire these bindings on property changed rather than simply on validation,
      // because on validation is not responsive enough. Also, painful technical note:
      // being a member of connectionTypeGroup *also* ensures that at most one of these buttons
      // are checked. So you might think databinding is not necessary. However being in
      // a group does nothing for the initial conditions. So the group doesn't care if
      // *neither* of them are checked.
      isCorePlusRadioButton.DataBindings.Add(new Binding(nameof(isCorePlusRadioButton.Checked),
        vm, nameof(vm.IsCorePlus), false, DataSourceUpdateMode.OnPropertyChanged));
      isCoreRadioButton.DataBindings.Add(new Binding(nameof(isCorePlusRadioButton.Checked),
        vm, nameof(vm.IsCore), false, DataSourceUpdateMode.OnPropertyChanged));

      // Make one of the two panels visible, according to the setting of the radio box.
      corePlusPanel.DataBindings.Add(nameof(corePlusPanel.Visible), vm, nameof(vm.IsCorePlus));
      corePanel.DataBindings.Add(nameof(corePanel.Visible), vm, nameof(vm.IsCore));

      // Bind the ID
      endpointIdBox.DataBindings.Add(nameof(endpointIdBox.Text), vm, nameof(vm.Id));

      // Bind the Core+ properties
      jsonUrlBox.DataBindings.Add(nameof(jsonUrlBox.Text), vm, nameof(vm.JsonUrl));
      userIdBox.DataBindings.Add(nameof(userIdBox.Text), vm, nameof(vm.UserId));
      passwordBox.DataBindings.Add(nameof(passwordBox.Text), vm, nameof(vm.Password));
      operateAsBox.DataBindings.Add(nameof(operateAsBox.Text), vm, nameof(vm.OperateAs));

      // Bind the Core property (there's just one)
      connectionStringBox.DataBindings.Add(nameof(connectionStringBox.Text),
        vm, nameof(vm.ConnectionString));

      // Bind the IsDefault property
      makeDefaultCheckBox.DataBindings.Add(nameof(makeDefaultCheckBox.Checked),
        vm, nameof(vm.IsDefault));
    }

    public void SetTestResultsBox(string painState) {
      Invoke(() => testResultsTextBox.Text = painState);
    }

    private void setCredentialsButton_Click(object sender, EventArgs e) {
      _onSetCredentialsButtonClicked();
    }

    private void testCredentialsButton_Click(object sender, EventArgs e) {
      _onTestCredentialsButtonClicked();
    }
  }
}
