using Deephaven.DeephavenClient;
using Deephaven.DheClient.Session;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Factories;

internal static class SessionBaseFactory {
  public static SessionBase Create(CredentialsBase credentials, WorkerThread workerThread) {
    return credentials.AcceptVisitor<SessionBase>(
      core => {
        var client = Client.Connect(core.ConnectionString, new ClientOptions());
        return new CoreSession(client);
      },

      corePlus => {
        var session = SessionManager.FromUrl("Deephaven Excel", corePlus.JsonUrl);
        if (!session.PasswordAuthentication(corePlus.User, corePlus.Password, corePlus.OperateAs)) {
          throw new Exception("Authentication failed");
        }

        return new CorePlusSession(session, workerThread);
      });
  }
}
