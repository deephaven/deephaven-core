# Background

The purpose of this library is to add a "Custom Action" to our
Advanced Installer package. This custom action does the actions
needed to manipulate the Windows Registry in order to do things like

1. detect whether the version of Office installed is 32 or 64 bit
2. Add the special keys that tell Excel to open an Excel Add-In at startup

# Information about this library

This library is a .NET 4.8.0 Class Library with some special boilerplate
code provided by Advanced Installer.

.NET 4.8.0 is pretty old at this point, but I chose it because (I believe)
it is guaranteed to be present on Windows 10/11 installations. Note that
this is *not* the runtime used by the Excel Add-In itself; that add-in uses
a much more modern runtime (.NET 8). This is just the runtime used to
support the custom actions (registry manipulations) in the installer.

This library and its boilerplate code were created by adding a
Visual Studio Extension provided by Advanced Installer to Visual Studio.
The process for adding the extension is documented here:

https://www.advancedinstaller.com/user-guide/create-dot-net-ca.html

Basically the steps are:

* Open Visual Studio and navigate to Extensions → Manage Extensions.
* In the Online section, search for Advanced Installer for Visual Studio
* In Visual Studio navigate to File → New Project
* From the list of templates, select the C# Custom Action template or the
  C# Custom Action (.NET Framework) template, depending on your needs

Because of the above compatibility requirements I have decided that
the right version is "C# Custom Action (.NET Framework)".

# Windows Registry

These are the reasons we need to access the Windows Registry

## Determining Office bitness

To determine the "bitness" (32 or 64) of the version of Office that is
installed, we look at this registry key:

```
HKEY_LOCAL_MACHINE\Software\Microsoft\Office\${VERSION}\Outlook
```

And yes, this information is stored at the "Outlook" part of the path,
not Excel. This key contains an entry with the name
which contains the name "Bitness" and the values "x86" or "x64".

When I say ${VERSION} I mean one of the known versions of Office, one of
the strings in the set 11.0, 12.0, 14.0, 15.0, 16.0

Version 16.0 covers Office 2016, 2019, and 2021 and Office 365, so
for Deephaven purposes we can hardcode this to 16.0 and ignore previous
versions we might find.

Note that for reasons when we look up this key programmatically, we need
to look it up in the "Registry Hive" that corresponds to the machine's
operating system bitness. This is why we have code like

```
var regView = Environment.Is64BitOperatingSystem ? RegistryView.Registry64 : RegistryView.Registry32;
var regBase = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, regView);
...
var bitnessValue = subKey.GetValue(RegistryKeys.Bitness.Name);
```

Apparently, the Bitness key for a 32 bit installation of Office will still
be in the 64 bit registry "hive" on a 64 bit machine. We may need to look
into this further if it turns out to matter.

## Modifying the set of installed Excel Add-Ins

The relevant registry key here is:

```
HKEY_CURRENT_USER\Software\Microsoft\Office\${VERSION}\Excel\Options
```

Again, VERSION is hardcoded to 16.0. Also, unlike the bitness step, we
don't have to write special code to pick a specific registry hive. This
is why we have code like

```
var subKey = Registry.CurrentUser.OpenSubKey(RegistryKeys.OpenEntries.Key, true);
```

This key contains zero or more entries indicating which addins Excel
should load when it starts. These entries have the following names, which follow the almost-regular pattern:

OPEN, OPEN1, OPEN2, OPEN3, ...

I say "almost-regular" because the first name is OPEN when you might
expect to to be named OPEN0.

These names must be kept dense. That is, if you delete some name that is
not at the end of the sequence, you will need to move the later entries
down to fill in the gap. (e.g. the entry keyed by OPEN2 becomes OPEN1 etc).

The value of these entries is a string that looks like the pattern

```
/R "${FULLPATHTOXLL}"
```

including the space and the quotation marks. On my computer the value of OPEN is currently

```
/R "C:\Users\kosak\Desktop\exceladdin-v7\ExcelAddIn-AddIn64-packed.xll"
```

The fact that I have installed my addin on the Desktop is not a best practice. The point here is to show the syntax.

We take care to make our entry follow the above format. Of course when we are moving
the entries installed by other people, we treat them as opaque strings and don't look
at the values.
