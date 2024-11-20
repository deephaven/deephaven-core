# Background

The purpose of this library is to add a "Custom Action" to our
Advanced Installer package. This custom action does the actions
needed to manipulate the Windows Registry in order to do things like

1. Detect whether the version of Office installed is 32 or 64 bit
2. Add the special keys that tell Excel to open an Excel Add-In at startup

# Information about this library

This library is a .NET 4.8.0 Class Library with some special boilerplate
code provided by Advanced Installer.

.NET 4.8.0 is pretty old at this point, but it was chosen because
it is guaranteed to be present on Windows 10/11 installations. Note that
this is *not* the runtime used by the Excel Add-In itself; that add-in uses
a much more modern runtime (.NET 8). This is just the runtime used to
support the custom actions (registry manipulations) in the installer.

This library and its boilerplate code were created by adding a
Visual Studio Extension provided by Advanced Installer to Visual Studio.
The process for adding the extension is documented here:

https://www.advancedinstaller.com/user-guide/create-dot-net-ca.html

The steps are:

* Open Visual Studio and navigate to Extensions → Manage Extensions.
* In the Browse tab, search for Advanced Installer for Visual Studio
  and press the Install button
* Close and reopen Visual Studio to finalize the installation.
* Now navigate to File → New → Project
* From the list of templates, you will find two very-similar looking
  templates:
  1. "C# Custom Action" with description ".Net Custom Action Project
     for Advanced Installer"
  2. "C# Custom Action (.NET Framework)" with description ".Net Framework
     Custom Action Project for Advanced Installer"

The difference between these two templates has to do with the
evolution of .NET. The original application framework
was called ".NET Framework" and it ran only on Windows. The modern,
cross-platform application framework is called simply .NET.
Because for our purposes here we have decided to target an old
.NET Framework version (4.8.0, see above), we choose the second
option: "C# Custom Action (.NET Framework)".

# Windows Registry

These are the reasons we need to access the Windows Registry

## Determining Office bitness

To determine the "bitness" (32 or 64) of the version of Office that is
installed, we look at this registry key:

```
HKEY_LOCAL_MACHINE\Software\Microsoft\Office\${VERSION}\Outlook
```

Notably, this information is stored at the "Outlook" part of the path,
not Excel. This key contains an entry with the name
"Bitness" and the values "x86" or "x64".

${VERSION} refers to one of the known versions of Office, namely one of
the strings in the set 11.0, 12.0, 14.0, 15.0, 16.0

Version 16.0 covers Office 2016, 2019, and 2021 and Office 365, so
for Deephaven purposes we can hardcode this to 16.0 and ignore previous
versions we might find.

As Windows evolved, the registry was divided into a 32-bit partition
and a 64-bit partition. Office itself is published in 32-bit and 64-bit versions.
The only configuration we currently support is 64-bit Office on 64-bit Windows.

Due to this registry organization, when we look up this key programmatically,
we need to look it up in the "Registry Hive" that corresponds to the machine's
operating system bitness. This is why we have code like

```
var regView = Environment.Is64BitOperatingSystem ? RegistryView.Registry64 : RegistryView.Registry32;
var regBase = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, regView);
...
var bitnessValue = subKey.GetValue(RegistryKeys.Bitness.Name);
```

However, this code is needlessly general because at this time we only support 64-bit Windows.

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
should load when it starts. These entries have the following names,
which follow the almost-regular pattern:

OPEN, OPEN1, OPEN2, OPEN3, ...

We say "almost-regular" because the first name is OPEN when you might
expect to to be named OPEN0.

These names must be kept dense. That is, if you delete some name that is
not at the end of the sequence, you will need to move the later entries
down to fill in the gap (e.g. the entry keyed by OPEN2 becomes OPEN1 etc).

The value of these entries is a string that looks like the pattern

```
/R "${FULLPATHTOXLL}"
```

including the space and the quotation marks. For example, for user "kosak" the
installer would make an entry that looks like this:

```
/R "C:\Users\kosak\AppData\Local\Deephaven Data Labs LLC\Deephaven Excel Add-In\DeephavenExcelAddIn64.xll"
```

The entry created by the installer follows the above format. Of course when we
have to move the entries installed by other software (e.g. if we have to
change OPEN3 to OPEN2), we treat the values as opaque strings.
