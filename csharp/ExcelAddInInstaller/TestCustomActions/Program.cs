using System;
using System.Diagnostics;

namespace Deephaven.ExcelAddInInstaller.CustomActions {
  public class Program {
    static void Main(string[] args) {
      Action<string> logger = Console.WriteLine;
      if (!RegistryManager.TryCreate(logger, out var oem, out var failureReason) ||
          !oem.TryUpdateAddInKeys("zamboni 666", false, out failureReason)) {
        logger($"Sad because {failureReason}");
        return;
      }

      logger("HAPPY");
    }
  }
}
