namespace Deephaven.DeephavenClient.ExcelAddIn.ViewModels;

public class EnterpriseConnectionDialogViewModel {
  private string _jsonUrl = EnterpriseConstants.DefaultJsonUrl;
  private string _userid = "";
  private string _password = "";
  private string _operateAs = "";

  public event EventHandler? JsonUrlChanged;
  public event EventHandler? UserIdChanged;
  public event EventHandler? PasswordChanged;
  public event EventHandler? OperateAsChanged;

  public string JsonUrl {
    get => _jsonUrl;
    set {
      if (_jsonUrl == value) {
        return;
      }
      _jsonUrl = value;
      JsonUrlChanged?.Invoke(this, EventArgs.Empty);
    }
  }

  public string UserId {
    get => _userid;
    set {
      if (_userid == value) {
        return;
      }
      _userid = value;
      UserIdChanged?.Invoke(this, EventArgs.Empty);
    }
  }

  public string Password {
    get => _password;
    set {
      if (_password == value) {
        return;
      }
      _password = value;
      PasswordChanged?.Invoke(this, EventArgs.Empty);
    }
  }

  public string OperateAs {
    get => _operateAs;
    set {
      if (_operateAs == value) {
        return;
      }
      _operateAs = value;
      OperateAsChanged?.Invoke(this, EventArgs.Empty);
    }
  }
}
