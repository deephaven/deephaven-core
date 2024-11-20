namespace Deephaven.ExcelAddInInstaller.CustomActions {
  public static class RegistryKeys {
    public static class Bitness {
      // Key is in HKEY_LOCAL_MACHINE
      public const string Key = @"Software\Microsoft\Office\16.0\Outlook";
      public const string Name = "Bitness";
      public const string Value64 = "x64";
      public const string Value32 = "x86";
    }

    public static class OpenEntries {
      // Key is in HKEY_CURRENT_USER
      public const string Key = @"Software\Microsoft\Office\16.0\Excel\Options";
    }
  }
}
