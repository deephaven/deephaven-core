namespace Deephaven.DeephavenClient.ExcelAddIn.Views {
  partial class ConnectionDialog {
    /// <summary>
    /// Required designer variable.
    /// </summary>
    private System.ComponentModel.IContainer components = null;

    /// <summary>
    /// Clean up any resources being used.
    /// </summary>
    /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
    protected override void Dispose(bool disposing) {
      if (disposing && (components != null)) {
        components.Dispose();
      }
      base.Dispose(disposing);
    }

    #region Windows Form Designer generated code

    /// <summary>
    /// Required method for Designer support - do not modify
    /// the contents of this method with the code editor.
    /// </summary>
    private void InitializeComponent() {
      connectButton = new Button();
      connectionStringText = new TextBox();
      connectionStringLabel = new Label();
      SuspendLayout();
      // 
      // connectButton
      // 
      connectButton.Location = new Point(254, 157);
      connectButton.Margin = new Padding(4, 5, 4, 5);
      connectButton.Name = "connectButton";
      connectButton.Size = new Size(125, 43);
      connectButton.TabIndex = 0;
      connectButton.Text = "Connect";
      connectButton.UseVisualStyleBackColor = true;
      connectButton.Click += connectButton_Click;
      // 
      // connectionStringText
      // 
      connectionStringText.Location = new Point(237, 35);
      connectionStringText.Name = "connectionStringText";
      connectionStringText.Size = new Size(264, 31);
      connectionStringText.TabIndex = 1;
      // 
      // connectionStringLabel
      // 
      connectionStringLabel.AutoSize = true;
      connectionStringLabel.Location = new Point(12, 38);
      connectionStringLabel.Name = "connectionStringLabel";
      connectionStringLabel.Size = new Size(153, 25);
      connectionStringLabel.TabIndex = 3;
      connectionStringLabel.Text = "Connection String";
      // 
      // ConnectionDialog
      // 
      AutoScaleDimensions = new SizeF(10F, 25F);
      AutoScaleMode = AutoScaleMode.Font;
      ClientSize = new Size(800, 450);
      Controls.Add(connectionStringLabel);
      Controls.Add(connectionStringText);
      Controls.Add(connectButton);
      Name = "ConnectionDialog";
      Text = "Connect to Deephaven Core";
      ResumeLayout(false);
      PerformLayout();
    }

    #endregion

    private Button connectButton;
    private TextBox connectionStringText;
    private Label connectionStringLabel;
  }
}