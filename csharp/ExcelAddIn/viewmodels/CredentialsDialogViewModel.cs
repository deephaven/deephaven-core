using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.ViewModels;

public sealed class CredentialsDialogViewModel : INotifyPropertyChanged {
  public static CredentialsDialogViewModel OfEmpty() {
    return new CredentialsDialogViewModel();
  }

  public static CredentialsDialogViewModel OfIdButOtherwiseEmpty(string id) {
    return new CredentialsDialogViewModel { Id = id };
  }

  public static CredentialsDialogViewModel OfIdAndCredentials(string id, CredentialsBase credentials) {
    var result = new CredentialsDialogViewModel {
      Id = credentials.Id.Id
    };
    _ = credentials.AcceptVisitor(
      core => {
        result._isCorePlus = false;
        result.ConnectionString = core.ConnectionString;
        result.SessionTypeIsPython = core.SessionTypeIsPython;
        return Unit.Instance;
      },
      corePlus => {
        result._isCorePlus = true;
        result.JsonUrl = corePlus.JsonUrl;
        result.UserId = corePlus.User;
        result.Password = corePlus.Password;
        result.OperateAs = corePlus.OperateAs.Equals(corePlus.User) ? "" : corePlus.OperateAs;
        result.ValidateCertificate = corePlus.ValidateCertificate;
        return Unit.Instance;
      });

    return result;
  }

  private string _id = "";
  private bool _isDefault = false;
  private bool _isCorePlus = true;
  private bool _sessionTypeIsPython = true;

  // Core properties
  private string _connectionString = "";

  // Core+ properties
  private string _jsonUrl = "";
  private bool _validateCertificate = true;
  private string _userId = "";
  private string _password = "";
  private string _operateAs = "";

  public event PropertyChangedEventHandler? PropertyChanged;

  public bool TryMakeCredentials([NotNullWhen(true)] out CredentialsBase? result,
    [NotNullWhen(false)] out string? errorText) {
    result = null;
    errorText = null;

    var missingFields = new List<string>();
    void CheckMissing(string field, string name) {
      if (field.Length == 0) {
        missingFields.Add(name);
      }
    }

    CheckMissing(Id, "Connection Id");

    if (!_isCorePlus) {
      CheckMissing(ConnectionString, "Connection String");
    } else {
      CheckMissing(JsonUrl, "JSON URL");
      CheckMissing(UserId, "User Id");
      CheckMissing(Password, "Password");
    }

    if (missingFields.Count > 0) {
      errorText = string.Join(Environment.NewLine, missingFields);
      return false;
    }

    var epId = new EndpointId(_id);
    result = _isCorePlus
      ? CredentialsBase.OfCorePlus(epId, JsonUrl, UserId, Password, OperateAsToUse, ValidateCertificate)
      : CredentialsBase.OfCore(epId, ConnectionString, SessionTypeIsPython);
    return true;
  }

  public string Id {
    get => _id;
    set {
      if (value == _id) {
        return;
      }

      _id = value;
      OnPropertyChanged();
    }
  }

  public bool IsDefault {
    get => _isDefault;
    set {
      if (value == _isDefault) {
        return;
      }

      _isDefault = value;
      OnPropertyChanged();
    }
  }

  /**
   * I don't know if I have to do it this way, but I bind IsCore and IsCorePlus to the
   * same underlying variable. The property "IsCore" maps to the inverse of the variable
   * _isCorePlus, meanwhile the property "IsCorePlus" maps to the normal sense of the
   * variable. Setters on either one trigger property change events for both.
   */
  public bool IsCore {
    get => !_isCorePlus;
    set {
      if (_isCorePlus == !value) {
        return;
      }

      _isCorePlus = !value;
      OnPropertyChanged();
      OnPropertyChanged(nameof(IsCorePlus));
    }
  }

  public bool IsCorePlus {
    get => _isCorePlus;
    set {
      if (_isCorePlus == value) {
        return;
      }

      _isCorePlus = value;
      OnPropertyChanged();
      OnPropertyChanged(nameof(IsCore));
    }
  }

  public string ConnectionString {
    get => _connectionString;
    set {
      if (_connectionString == value) {
        return;
      }

      _connectionString = value;
      OnPropertyChanged();
    }
  }

  public string JsonUrl {
    get => _jsonUrl;
    set {
      if (_jsonUrl == value) {
        return;
      }

      _jsonUrl = value;
      OnPropertyChanged();
    }
  }

  public string UserId {
    get => _userId;
    set {
      if (_userId == value) {
        return;
      }

      _userId = value;
      OnPropertyChanged();
    }
  }

  public string Password {
    get => _password;
    set {
      if (_password == value) {
        return;
      }

      _password = value;
      OnPropertyChanged();
    }
  }

  public string OperateAs {
    get => _operateAs;
    set {
      if (_operateAs == value) {
        return;
      }

      _operateAs = value;
      OnPropertyChanged();
    }
  }

  public string OperateAsToUse => _operateAs.Length != 0 ? _operateAs : UserId;

  public bool ValidateCertificate {
    get => _validateCertificate;
    set {
      if (_validateCertificate == value) {
        return;
      }

      _validateCertificate = value;
      OnPropertyChanged();
    }
  }

  public bool SessionTypeIsPython {
    get => _sessionTypeIsPython;
    set {
      if (_sessionTypeIsPython == value) {
        return;
      }

      _sessionTypeIsPython = value;
      OnPropertyChanged();
      OnPropertyChanged(nameof(SessionTypeIsGroovy));
    }
  }

  public bool SessionTypeIsGroovy {
    get => !_sessionTypeIsPython;
    set {
      if (!_sessionTypeIsPython == value) {
        return;
      }

      _sessionTypeIsPython = !value;
      OnPropertyChanged();
      OnPropertyChanged(nameof(SessionTypeIsPython));
    }
  }

  private void OnPropertyChanged([CallerMemberName] string? name = null) {
    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
  }
}
