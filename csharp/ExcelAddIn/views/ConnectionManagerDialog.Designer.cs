namespace Deephaven.ExcelAddIn.Views {
  partial class ConnectionManagerDialog {
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
      colorDialog1 = new ColorDialog();
      dataGridView1 = new DataGridView();
      newButton = new Button();
      connectionsLabel = new Label();
      ((System.ComponentModel.ISupportInitialize)dataGridView1).BeginInit();
      SuspendLayout();
      // 
      // dataGridView1
      // 
      dataGridView1.AllowUserToAddRows = false;
      dataGridView1.AllowUserToDeleteRows = false;
      dataGridView1.ColumnHeadersHeightSizeMode = DataGridViewColumnHeadersHeightSizeMode.AutoSize;
      dataGridView1.Location = new Point(68, 83);
      dataGridView1.Name = "dataGridView1";
      dataGridView1.ReadOnly = true;
      dataGridView1.RowHeadersWidth = 62;
      dataGridView1.Size = new Size(979, 454);
      dataGridView1.TabIndex = 0;
      // 
      // newButton
      // 
      newButton.Location = new Point(869, 560);
      newButton.Name = "newButton";
      newButton.Size = new Size(178, 34);
      newButton.TabIndex = 1;
      newButton.Text = "New Connection";
      newButton.UseVisualStyleBackColor = true;
      newButton.Click += newButton_Click;
      // 
      // connectionsLabel
      // 
      connectionsLabel.AutoSize = true;
      connectionsLabel.Font = new Font("Segoe UI", 12F, FontStyle.Regular, GraphicsUnit.Point, 0);
      connectionsLabel.Location = new Point(68, 33);
      connectionsLabel.Name = "connectionsLabel";
      connectionsLabel.Size = new Size(147, 32);
      connectionsLabel.TabIndex = 2;
      connectionsLabel.Text = "Connections";
      // 
      // ConnectionManagerDialog
      // 
      AutoScaleDimensions = new SizeF(10F, 25F);
      AutoScaleMode = AutoScaleMode.Font;
      ClientSize = new Size(1115, 615);
      Controls.Add(connectionsLabel);
      Controls.Add(newButton);
      Controls.Add(dataGridView1);
      Name = "ConnectionManagerDialog";
      Text = "Connection Manager";
      ((System.ComponentModel.ISupportInitialize)dataGridView1).EndInit();
      ResumeLayout(false);
      PerformLayout();
    }

    #endregion

    private ColorDialog colorDialog1;
    private DataGridView dataGridView1;
    private Button newButton;
    private Label connectionsLabel;
  }
}