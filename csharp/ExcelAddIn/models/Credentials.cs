namespace Deephaven.ExcelAddIn.Models;

public abstract class CredentialsBase(EndpointId id) {
  public readonly EndpointId Id = id;

  public static CredentialsBase OfCore(EndpointId id, string connectionString, bool sessionTypeIsPython) {
    return new CoreCredentials(id, connectionString, sessionTypeIsPython);
  }

  public static CredentialsBase OfCorePlus(EndpointId id, string jsonUrl, string userId,
    string password, string operateAs, bool validateCertificate) {
    return new CorePlusCredentials(id, jsonUrl, userId, password, operateAs, validateCertificate);
  }

  public abstract T AcceptVisitor<T>(Func<CoreCredentials, T> ofCore,
    Func<CorePlusCredentials, T> ofCorePlus);
}

public sealed class CoreCredentials(
  EndpointId id,
  string connectionString,
  bool sessionTypeIsPython) : CredentialsBase(id) {
  public readonly string ConnectionString = connectionString;
  public readonly bool SessionTypeIsPython = sessionTypeIsPython;

  public override T AcceptVisitor<T>(Func<CoreCredentials, T> ofCore, Func<CorePlusCredentials, T> ofCorePlus) {
    return ofCore(this);
  }
}

public sealed class CorePlusCredentials(EndpointId id, string jsonUrl, string user, string password,
  string operateAs, bool validateCertificate) : CredentialsBase(id) {
  public readonly string JsonUrl = jsonUrl;
  public readonly string User = user;
  public readonly string Password = password;
  public readonly string OperateAs = operateAs;
  public readonly bool ValidateCertificate = validateCertificate;

  public override T AcceptVisitor<T>(Func<CoreCredentials, T> ofCore, Func<CorePlusCredentials, T> ofCorePlus) {
    return ofCorePlus(this);
  }
}
