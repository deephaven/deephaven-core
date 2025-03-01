namespace ExcelAddIn.views {
  partial class DeephavenMessageBox {
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
      captionLabel = new Label();
      contentsBox = new TextBox();
      okButton = new Button();
      captionPanel = new Panel();
      cancelButton = new Button();
      captionPanel.SuspendLayout();
      SuspendLayout();
      // 
      // captionLabel
      // 
      captionLabel.Dock = DockStyle.Fill;
      captionLabel.Font = new Font("Segoe UI", 20F, FontStyle.Regular, GraphicsUnit.Point, 0);
      captionLabel.Location = new Point(0, 0);
      captionLabel.Name = "captionLabel";
      captionLabel.Size = new Size(751, 73);
      captionLabel.TabIndex = 0;
      captionLabel.Text = "Caption";
      captionLabel.TextAlign = ContentAlignment.MiddleCenter;
      // 
      // contentsBox
      // 
      contentsBox.Anchor = AnchorStyles.Top | AnchorStyles.Bottom | AnchorStyles.Left | AnchorStyles.Right;
      contentsBox.Location = new Point(40, 105);
      contentsBox.Multiline = true;
      contentsBox.Name = "contentsBox";
      contentsBox.ReadOnly = true;
      contentsBox.Size = new Size(716, 207);
      contentsBox.TabIndex = 1;
      contentsBox.TabStop = false;
      // 
      // okButton
      // 
      okButton.Anchor = AnchorStyles.Bottom | AnchorStyles.Right;
      okButton.Location = new Point(644, 343);
      okButton.Name = "okButton";
      okButton.Size = new Size(112, 34);
      okButton.TabIndex = 2;
      okButton.Text = "OK";
      okButton.UseVisualStyleBackColor = true;
      okButton.Click += okButton_Click;
      // 
      // captionPanel
      // 
      captionPanel.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right;
      captionPanel.Controls.Add(captionLabel);
      captionPanel.Location = new Point(24, 12);
      captionPanel.Name = "captionPanel";
      captionPanel.Size = new Size(751, 73);
      captionPanel.TabIndex = 3;
      // 
      // cancelButton
      // 
      cancelButton.Anchor = AnchorStyles.Bottom | AnchorStyles.Right;
      cancelButton.Location = new Point(513, 343);
      cancelButton.Name = "cancelButton";
      cancelButton.Size = new Size(112, 34);
      cancelButton.TabIndex = 4;
      cancelButton.Text = "Cancel";
      cancelButton.UseVisualStyleBackColor = true;
      cancelButton.Click += cancelButton_Click;
      // 
      // DeephavenMessageBox
      // 
      AutoScaleDimensions = new SizeF(10F, 25F);
      AutoScaleMode = AutoScaleMode.Font;
      ClientSize = new Size(800, 401);
      Controls.Add(cancelButton);
      Controls.Add(captionPanel);
      Controls.Add(okButton);
      Controls.Add(contentsBox);
      Name = "DeephavenMessageBox";
      Text = "Deephaven Message";
      captionPanel.ResumeLayout(false);
      ResumeLayout(false);
      PerformLayout();
    }

    #endregion

    private Label captionLabel;
    private TextBox contentsBox;
    private Button okButton;
    private Panel captionPanel;
    private Button cancelButton;
  }
}