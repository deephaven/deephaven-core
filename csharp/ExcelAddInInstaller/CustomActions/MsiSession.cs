using System;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace Deephaven.ExcelAddInInstaller.CustomActions {
  public class MsiSession {
    public class NativeMethods {
      public const ulong WS_VISIBLE = 0x10000000L;

      public const int GWL_STYLE = -16;

      // Declare the delegate for EnumWindows callback
      public delegate bool EnumWindowsCallback(IntPtr hwnd, int lParam);

      // Import the user32.dll library
      [DllImport("user32.dll")]
      public static extern int EnumWindows(EnumWindowsCallback callback, int lParam);

      [DllImport("user32.dll")]
      public static extern uint GetWindowThreadProcessId(IntPtr hWnd, out uint lpdwProcessId);

      [DllImport("user32.dll", SetLastError = true)]
      public static extern UInt32 GetWindowLong(IntPtr hWnd, int nIndex);

      [DllImport("msi.dll", CharSet = CharSet.Unicode)]
      public static extern uint MsiGetProperty(
        int hInstall,
        string szName,
        StringBuilder szValueBuf,
        ref uint pcchValueBuf);

      [DllImport("msi.dll", CharSet = CharSet.Unicode)]
      public static extern uint MsiSetProperty(int hInstall, string szName, string szValue);

      [DllImport("msi.dll", CharSet = CharSet.Unicode)]
      public static extern int MsiCreateRecord(uint cParams);

      [DllImport("msi.dll", CharSet = CharSet.Unicode)]
      public static extern uint MsiRecordSetString(int hRecord, uint iField, string szValue);

      [DllImport("msi.dll", CharSet = CharSet.Unicode)]
      public static extern int MsiProcessMessage(int hInstall, uint eMessageType, int hRecord);
    }

    public enum InstallMessage : uint {
      FATALEXIT = 0x00000000, // premature termination, possibly fatal OOM
      ERROR = 0x01000000, // formatted error message
      WARNING = 0x02000000, // formatted warning message
      USER = 0x03000000, // user request message
      INFO = 0x04000000, // informative message for log
      FILESINUSE = 0x05000000, // list of files in use that need to be replaced
      RESOLVESOURCE = 0x06000000, // request to determine a valid source location
      OUTOFDISKSPACE = 0x07000000, // insufficient disk space message
      ACTIONSTART = 0x08000000, // start of action: action name & description
      ACTIONDATA = 0x09000000, // formatted data associated with individual action item
      PROGRESS = 0x0A000000, // progress gauge info: units so far, total
      COMMONDATA = 0x0B000000, // product info for dialog: language Id, dialog caption
      INITIALIZE = 0x0C000000, // sent prior to UI initialization, no string data
      TERMINATE = 0x0D000000, // sent after UI termination, no string data
      SHOWDIALOG = 0x0E000000, // sent prior to display or authored dialog or wizard
    }

    private IntPtr mMsiWindowHandle = IntPtr.Zero;

    private bool EnumWindowCallback(IntPtr hwnd, int lParam) {
      uint wnd_proc = 0;
      NativeMethods.GetWindowThreadProcessId(hwnd, out wnd_proc);

      if (wnd_proc == lParam) {
        UInt32 style = NativeMethods.GetWindowLong(hwnd, NativeMethods.GWL_STYLE);
        if ((style & NativeMethods.WS_VISIBLE) != 0) {
          mMsiWindowHandle = hwnd;
          return false;
        }
      }

      return true;
    }

    public IntPtr MsiHandle { get; private set; }

    public string CustomActionData { get; private set; }

    public MsiSession(string aMsiHandle) {
      if (string.IsNullOrEmpty(aMsiHandle))
        throw new ArgumentNullException();

      int msiHandle = 0;
      if (!int.TryParse(aMsiHandle, out msiHandle))
        throw new ArgumentException("Invalid msi handle");

      MsiHandle = new IntPtr(msiHandle);

      string allData = GetProperty("CustomActionData");
      CustomActionData = allData.Split(new char[] { '|' }).First();
    }

    public string GetProperty(string aProperty) {
      // Get buffer size
      uint pSize = 0;
      StringBuilder valueBuffer = new StringBuilder();
      NativeMethods.MsiGetProperty(MsiHandle.ToInt32(), aProperty, valueBuffer, ref pSize);

      // Get property value
      pSize++; // null terminated
      valueBuffer.Capacity = (int)pSize;
      NativeMethods.MsiGetProperty(MsiHandle.ToInt32(), aProperty, valueBuffer, ref pSize);

      return valueBuffer.ToString();
    }

    public void SetProperty(string aProperty, string aValue) {
      NativeMethods.MsiSetProperty(MsiHandle.ToInt32(), aProperty, aValue);
    }

    public void Log(string aMessage, InstallMessage aMessageType) {
      int hRecord = NativeMethods.MsiCreateRecord(1);
      NativeMethods.MsiRecordSetString(hRecord, 0, "[1]");
      NativeMethods.MsiRecordSetString(hRecord, 1, aMessage);
      NativeMethods.MsiProcessMessage(MsiHandle.ToInt32(), (uint)aMessageType, hRecord);
    }

    public IntPtr GetMsiWindowHandle() {
      string msiProcId = GetProperty("CLIENTPROCESSID");
      if (string.IsNullOrEmpty(msiProcId))
        return IntPtr.Zero;

      IntPtr handle = new IntPtr(Convert.ToInt32(msiProcId));
      mMsiWindowHandle = IntPtr.Zero;
      NativeMethods.EnumWindows(EnumWindowCallback, (int)handle);

      return mMsiWindowHandle;
    }
  }
}
