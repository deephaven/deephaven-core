namespace ExcelAddIn.views {
  partial class EnterpriseConnectionDialog {
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
      label1 = new Label();
      label2 = new Label();
      label3 = new Label();
      label4 = new Label();
      label5 = new Label();
      jsonUrlText = new TextBox();
      operateAsText = new TextBox();
      persistentQueryText = new TextBox();
      passwordText = new TextBox();
      usernameText = new TextBox();
      connectButton = new Button();
      label6 = new Label();
      statusText = new Label();
      SuspendLayout();
      // 
      // label1
      // 
      label1.AutoSize = true;
      label1.Location = new Point(29, 45);
      label1.Name = "label1";
      label1.Size = new Size(91, 25);
      label1.TabIndex = 0;
      label1.Text = "JSON URL";
      // 
      // label2
      // 
      label2.AutoSize = true;
      label2.Location = new Point(29, 94);
      label2.Name = "label2";
      label2.Size = new Size(91, 25);
      label2.TabIndex = 1;
      label2.Text = "Username";
      // 
      // label3
      // 
      label3.AutoSize = true;
      label3.Location = new Point(29, 143);
      label3.Name = "label3";
      label3.Size = new Size(87, 25);
      label3.TabIndex = 2;
      label3.Text = "Password";
      // 
      // label4
      // 
      label4.AutoSize = true;
      label4.Location = new Point(29, 192);
      label4.Name = "label4";
      label4.Size = new Size(96, 25);
      label4.TabIndex = 3;
      label4.Text = "OperateAs";
      // 
      // label5
      // 
      label5.AutoSize = true;
      label5.Location = new Point(29, 241);
      label5.Name = "label5";
      label5.Size = new Size(140, 25);
      label5.TabIndex = 4;
      label5.Text = "Persistent Query";
      // 
      // jsonUrlText
      // 
      jsonUrlText.Location = new Point(184, 39);
      jsonUrlText.Name = "jsonUrlText";
      jsonUrlText.Size = new Size(433, 31);
      jsonUrlText.TabIndex = 5;
      // 
      // operateAsText
      // 
      operateAsText.Location = new Point(184, 189);
      operateAsText.Name = "operateAsText";
      operateAsText.Size = new Size(433, 31);
      operateAsText.TabIndex = 6;
      // 
      // persistentQueryText
      // 
      persistentQueryText.Location = new Point(184, 241);
      persistentQueryText.Name = "persistentQueryText";
      persistentQueryText.Size = new Size(433, 31);
      persistentQueryText.TabIndex = 7;
      // 
      // passwordText
      // 
      passwordText.Location = new Point(184, 140);
      passwordText.Name = "passwordText";
      passwordText.Size = new Size(433, 31);
      passwordText.TabIndex = 8;
      // 
      // usernameText
      // 
      usernameText.Location = new Point(184, 88);
      usernameText.Name = "usernameText";
      usernameText.Size = new Size(433, 31);
      usernameText.TabIndex = 9;
      // 
      // connectButton
      // 
      connectButton.Location = new Point(270, 305);
      connectButton.Name = "connectButton";
      connectButton.Size = new Size(223, 64);
      connectButton.TabIndex = 10;
      connectButton.Text = "Connect To Deephaven Enterprise";
      connectButton.UseVisualStyleBackColor = true;
      // 
      // label6
      // 
      label6.AutoSize = true;
      label6.Location = new Point(29, 404);
      label6.Name = "label6";
      label6.Size = new Size(64, 25);
      label6.TabIndex = 11;
      label6.Text = "Status:";
      // 
      // statusText
      // 
      statusText.AutoSize = true;
      statusText.Location = new Point(184, 404);
      statusText.Name = "statusText";
      statusText.Size = new Size(0, 25);
      statusText.TabIndex = 12;
      // 
      // EnterpriseConnectionDialog
      // 
      AutoScaleDimensions = new SizeF(10F, 25F);
      AutoScaleMode = AutoScaleMode.Font;
      ClientSize = new Size(800, 450);
      Controls.Add(statusText);
      Controls.Add(label6);
      Controls.Add(connectButton);
      Controls.Add(usernameText);
      Controls.Add(passwordText);
      Controls.Add(persistentQueryText);
      Controls.Add(operateAsText);
      Controls.Add(jsonUrlText);
      Controls.Add(label5);
      Controls.Add(label4);
      Controls.Add(label3);
      Controls.Add(label2);
      Controls.Add(label1);
      Name = "EnterpriseConnectionDialog";
      Text = "EnterpriseConnectionDialog";
      ResumeLayout(false);
      PerformLayout();
    }

    #endregion

    private Label label1;
    private Label label2;
    private Label label3;
    private Label label4;
    private Label label5;
    private TextBox jsonUrlText;
    private TextBox operateAsText;
    private TextBox persistentQueryText;
    private TextBox passwordText;
    private TextBox usernameText;
    private Button connectButton;
    private Label label6;
    private Label statusText;
  }
}