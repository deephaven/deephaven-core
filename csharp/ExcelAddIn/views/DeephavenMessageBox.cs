using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace ExcelAddIn.views {
  public partial class DeephavenMessageBox : Form {
    public DeephavenMessageBox(string caption, string text, bool cancelVisible) {
      InitializeComponent();

      captionLabel.Text = caption;
      contentsBox.Text = text;
      cancelButton.Visible = cancelVisible;

      AcceptButton = AcceptButton;
      CancelButton = cancelButton;
    }

    private void okButton_Click(object sender, EventArgs e) {
      DialogResult = DialogResult.OK;
      Close();
    }

    private void cancelButton_Click(object sender, EventArgs e) {
      DialogResult = DialogResult.Cancel;
      Close();
    }
  }
}
