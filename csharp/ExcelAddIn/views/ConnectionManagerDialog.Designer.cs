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
      editButton = new Button();
      deleteButton = new Button();
      reconnectButton = new Button();
      makeDefaultButton = new Button();
      ((System.ComponentModel.ISupportInitialize)dataGridView1).BeginInit();
      SuspendLayout();
      // 
      // dataGridView1
      // 
      dataGridView1.AllowUserToAddRows = false;
      dataGridView1.AllowUserToDeleteRows = false;
      dataGridView1.Anchor = AnchorStyles.Top | AnchorStyles.Bottom | AnchorStyles.Left | AnchorStyles.Right;
      dataGridView1.AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.Fill;
      dataGridView1.ColumnHeadersHeightSizeMode = DataGridViewColumnHeadersHeightSizeMode.AutoSize;
      dataGridView1.Location = new Point(68, 83);
      dataGridView1.Name = "dataGridView1";
      dataGridView1.ReadOnly = true;
      dataGridView1.RowHeadersWidth = 62;
      dataGridView1.SelectionMode = DataGridViewSelectionMode.FullRowSelect;
      dataGridView1.Size = new Size(979, 454);
      dataGridView1.TabIndex = 0;
      // 
      // newButton
      // 
      newButton.Anchor = AnchorStyles.Bottom | AnchorStyles.Right;
      newButton.Location = new Point(919, 560);
      newButton.Name = "newButton";
      newButton.Size = new Size(128, 34);
      newButton.TabIndex = 5;
      newButton.Text = "New...";
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
      // editButton
      // 
      editButton.Anchor = AnchorStyles.Bottom | AnchorStyles.Right;
      editButton.Location = new Point(776, 560);
      editButton.Name = "editButton";
      editButton.Size = new Size(112, 34);
      editButton.TabIndex = 4;
      editButton.Text = "Edit...";
      editButton.UseVisualStyleBackColor = true;
      editButton.Click += editButton_Click;
      // 
      // deleteButton
      // 
      deleteButton.Anchor = AnchorStyles.Bottom | AnchorStyles.Right;
      deleteButton.Location = new Point(339, 560);
      deleteButton.Name = "deleteButton";
      deleteButton.Size = new Size(112, 34);
      deleteButton.TabIndex = 1;
      deleteButton.Text = "Delete";
      deleteButton.UseVisualStyleBackColor = true;
      deleteButton.Click += deleteButton_Click;
      // 
      // reconnectButton
      // 
      reconnectButton.Anchor = AnchorStyles.Bottom | AnchorStyles.Right;
      reconnectButton.Location = new Point(636, 560);
      reconnectButton.Name = "reconnectButton";
      reconnectButton.Size = new Size(112, 34);
      reconnectButton.TabIndex = 3;
      reconnectButton.Text = "Reconnect";
      reconnectButton.UseVisualStyleBackColor = true;
      reconnectButton.Click += reconnectButton_Click;
      // 
      // makeDefaultButton
      // 
      makeDefaultButton.Anchor = AnchorStyles.Bottom | AnchorStyles.Right;
      makeDefaultButton.Location = new Point(473, 560);
      makeDefaultButton.Name = "makeDefaultButton";
      makeDefaultButton.Size = new Size(139, 34);
      makeDefaultButton.TabIndex = 2;
      makeDefaultButton.Text = "Make Default";
      makeDefaultButton.UseVisualStyleBackColor = true;
      makeDefaultButton.Click += makeDefaultButton_Click;
      // 
      // ConnectionManagerDialog
      // 
      AutoScaleDimensions = new SizeF(10F, 25F);
      AutoScaleMode = AutoScaleMode.Font;
      ClientSize = new Size(1115, 615);
      Controls.Add(makeDefaultButton);
      Controls.Add(reconnectButton);
      Controls.Add(deleteButton);
      Controls.Add(editButton);
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
    private Button editButton;
    private Button deleteButton;
    private Button reconnectButton;
    private Button makeDefaultButton;
  }
}