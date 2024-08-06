namespace Deephaven.DeephavenClient.ExcelAddIn.ViewModels;

public class EnterpriseConnectionDialogViewModel {
  private string _jsonUrl = EnterpriseConstants.DefaultJsonUrl;
  private string _userid = "iris";
  private string _password = "iris";
  private string _operateAs = "iris";
  private string _persistentQuery = "zamboni1";

  public event EventHandler? JsonUrlChanged;
  public event EventHandler? UserIdChanged;
  public event EventHandler? PasswordChanged;
  public event EventHandler? OperateAsChanged;
  public event EventHandler? PersistentQueryChanged;

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

  public string PersistentQuery {
    get => _persistentQuery;
    set {
      if (_persistentQuery == value) {
        return;
      }
      _persistentQuery = value;
      PersistentQueryChanged?.Invoke(this, EventArgs.Empty);
    }
  }
}
