using Deephaven.ExcelAddIn.ViewModels;

namespace ExcelAddIn.views {
  public partial class CredentialsDialog : Form {
    public event Action? OnSetCredentialsButtonClicked = null;
    public event Action? OnTestCredentialsButtonClicked = null;

    public CredentialsDialog(CredentialsDialogViewModel vm) {
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

      validateCertCheckBox.DataBindings.Add(nameof(validateCertCheckBox.Checked), vm, nameof(vm.ValidateCertificate));

      // Bind the Core property (there's just one)
      connectionStringBox.DataBindings.Add(nameof(connectionStringBox.Text),
        vm, nameof(vm.ConnectionString));

      // Bind the SessionType checkboxes
      sessionTypeIsPythonButton.DataBindings.Add(nameof(sessionTypeIsPythonButton.Checked),
        vm, nameof(vm.SessionTypeIsPython));
      sessionTypeIsGroovyButton.DataBindings.Add(nameof(sessionTypeIsGroovyButton.Checked),
        vm, nameof(vm.SessionTypeIsGroovy));

      // Bind the IsDefault property
      makeDefaultCheckBox.DataBindings.Add(nameof(makeDefaultCheckBox.Checked),
        vm, nameof(vm.IsDefault));
    }

    public void SetTestResultsBox(string testResultsState) {
      if (InvokeRequired) {
        try {
          Invoke(() => SetTestResultsBox(testResultsState));
        } catch (InvalidOperationException) {
          // TODO(kosak): figure out a better way to handle this race
          // Invoke will fail if it is called after the form was closed. There is a race
          // here that is pretty hard to get rid of. For now we catch the exception and
          // throw away the value.
        }
        return;
      }

      testResultsTextBox.Text = testResultsState;
    }

    private void setCredentialsButton_Click(object sender, EventArgs e) {
      OnSetCredentialsButtonClicked?.Invoke();
    }

    private void testCredentialsButton_Click(object sender, EventArgs e) {
      OnTestCredentialsButtonClicked?.Invoke();
    }
  }
}
