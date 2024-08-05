using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using Deephaven.DeephavenClient.ExcelAddIn.ViewModels;

namespace ExcelAddIn.views {
  public partial class EnterpriseConnectionDialog : Form {
    private readonly Action<Form, string> _onConnect;

    public EnterpriseConnectionDialog(EnterpriseConnectionDialogViewModel vm,
      Action<Form, string> onConnect) {
      _onConnect = onConnect;
      InitializeComponent();
      jsonUrlText.DataBindings.Add("Text", vm, "JsonUrl");
     // userIdText.DataBindings.Add("Text", vm, "JsonUrl");
    }

    private void connectButton_Click(object sender, EventArgs e) {
      //_onConnect(this, this.connectionStringText.Text.Trim());
    }
  }
}
