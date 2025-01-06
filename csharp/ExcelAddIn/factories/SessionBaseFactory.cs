using Deephaven.DeephavenClient;
using Deephaven.DheClient.Session;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Factories;

internal static class SessionBaseFactory {
  public static SessionBase Create(CredentialsBase credentials, WorkerThread workerThread) {
    return credentials.AcceptVisitor<SessionBase>(
      core => {
        var options = new ClientOptions();
        options.SetSessionType(core.SessionTypeIsPython ? "python" : "groovy");
        var client = Client.Connect(core.ConnectionString, options);
        return new CoreSession(client);
      },

      corePlus => {
        var handler = new HttpClientHandler();
        if (!corePlus.ValidateCertificate) {
          handler.ClientCertificateOptions = ClientCertificateOption.Manual;
          handler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;
        }
        var hc = new HttpClient(handler);
        var json = hc.GetStringAsync(corePlus.JsonUrl).Result;
        var session = SessionManager.FromJson("Deephaven Excel", json);
        if (!session.PasswordAuthentication(corePlus.User, corePlus.Password, corePlus.OperateAs)) {
          throw new Exception("Authentication failed");
        }

        return new CorePlusSession(session, workerThread);
      });
  }
}
