# Building the Excel Add-In on Windows 10 / 11.

These instructions show how to install and run the Deephaven Excel Add-In
on Windows. These instructions also happen to build the Deephaven C# Client as a
side-effect. However if your goal is only to build the Deephaven C# Client,
please see the instructions at %DHSRC%\csharp\client\README.md
(does not exist yet).

We have tested these instructions on Windows 10 and 11 with Visual Studio
Community Edition.

# Before using the Excel Add-In

To actually use the Deephaven Excel Add-In, you will eventually need to have
at least one Community Core or Enterprise Core+ server running. You don't need
the server yet, and you can successfully follow these build instructions
without a server. However, you will eventually need a server when you want to
run it.

If you don't have a Deephaven Community Core server installation,
you can use these instructions to build one.
https://deephaven.io/core/docs/getting-started/launch-build/

Note that it is only possible to build a server on Linux. Building a server
on Windows is not currently supported.

For Deephaven Enterprise Core+, contact your IT administrator.

# Building the Excel Add-In on Windows 10 / Windows 11

## Prerequisites

## Build machine specifications

In our experience following this instructions on a fresh Windows 11 VM
required a total of 125G of disk space to install and build everything.
We recommend a machine with at least 200G of free disk space in order to
leave a comfortable margin.

Also, building the dependencies with vcpkg is very resource-intensive.
A machine that has more cores will be able to finish faster.
We recommend at least 16 cores for the build process. Note that *running*
the Excel Add-In does not require that many cores.

## Tooling

### Excel

You will need a recent version of Excel installed. We recommend Office 21
or Office 365. Note that the Add-In only works with locally-installed
versions of Excel (i.e. software installed on your computer). It does not
work with the browser-based web version of Excel.

### .NET

Install the .NET Core SDK, version 8.0.
Look for the "Windows | x64" link
[here](https://dotnet.microsoft.com/en-us/download/dotnet/8.0)

### Visual Studio

Install Visual Studio 2022 Community Edition (or Professional, or Enterprise)
from [here](https://visualstudio.microsoft.com/downloads/)

When the installer runs, select both workloads
"Desktop development with C++" and ".NET desktop development".

If Visual Studio is already installed, use Tools -> Get Tools and Features
to add those workloads if necessary.

### git

Use your preferred version of git, or install Git from
[here](https://git-scm.com/download/win)

## C++ client

The Deephaven Excel Add-In relies on the Deephaven C# Client, which in turn
requires the Deephaven C++ Client (Community Core version). Furthermore, if
you want to use Enterprise Core+ features, you also need the Deephaven C++
Client for Enterprise Core+.

The instructions below describe how to build these libraries.

### Build the Deephaven C++ Client (Community Core version)

Follow the instructions at [repository root]/cpp-client/README.md under the
section, under "Building the C++ client on Windows 10 / Windows 11".

When that process is done, you will have C++ client binaries in the
directory you specified in your DHINSTALL environment variable.

### (Optional) build the Deephaven C++ Client (Enterprise Core+ version)

To access Enterprise features, build the Enterprise Core+ version as well.
It will also store its binaries in the same DHINSTALL directory.

(instructions TODO)

## Build the Excel Add-In and C# Add-In

You can build the Add-In from inside Visual Studio or from the Visual Studio
Command Prompt.

### From within Visual Studio

1. Open the Visual Studio solution file
[repository root]\csharp\ExcelAddIn\ExcelAddIn.sln

2. Click on BUILD -> Build solution

### From the Visual Studio Command Prompt

```
cd [repository root]\csharp\ExcelAddIn
devenv ExcelAddIn.sln /build Release
```

## Run the Add-In

### Environment variables

Set these variables (or keep them set from the above steps) to the locations
of your Deephaven source directory and your Deephaven install directory.

```
set DHSRC=%HOMEDRIVE%%HOMEPATH%\dhsrc
set DHINSTALL=%HOMEDRIVE%%HOMEPATH%\dhinstall
```

### Running from within Visual Studio

1. In order to actually function, the Add-In requires the C++ Client binaries
   built in the above steps. The easiest thing to do is simply copy all the
   binaries into your Visual Studio build directory:

Assuming a Debug build:

copy /Y %DHINSTALL%\bin %DHSRC%\csharp\ExcelAddIn\bin\Debug\net8.0-windows

If you are doing a Release build, change "Debug" to "Release" in the above path.

2. Inside Visual Studio Select Debug -> Start Debugging

Visual Studio will launch Excel automatically. Excel will launch with a
Security Notice because the add-in is not signed. Click "Enable this add-in
for this session only."

### From standalone Excel

To install the add-in into Excel, we need put the relevant files into a
directory and then point Excel to that directory. These steps assume you
have already built the C# project with Visual Studio.

For simplicity, we will make a folder on the Desktop called "exceladdin".

```
set ADDINDIR=%HOMEDRIVE%%HOMEPATH%\Desktop\exceladdin
mkdir %ADDINDIR%
```

Now copy the C++ files, the C# files, and the XLL file to this directory:
```
copy /Y %DHINSTALL%\bin\* %ADDINDIR%
copy /Y %DHSRC%\csharp\ExcelAddIn\bin\Debug\net8.0-windows\* %ADDINDIR%
copy /Y %DHSRC%\csharp\ExcelAddIn\bin\Debug\net8.0-windows\publish\ExcelAddIn-Addin64-packed.xll %ADDINDIR%
```

As above, if you happen to have done a Release build in C#, then change Debug to Release in the above paths.

Then, run Excel and follow the following steps.

1. Click on "Options" at the lower left of the window.
2. Click on "Add-ins" on the left, second from the bottom.
3. At the bottom of the screen click, near "Manage", select "Excel Add-ins"
   from the pulldown, and then click "Go..."
4. In the next screen click "Browse..." 
5. Navigate to your %ADDINDIR% directory and click on the ExcelAddIn-Addin64-packed.xll file that you recently copied there
6. Click OK


## Test the add-in

### Without connecting to a Deephaven server

1. In Excel, click on Add-ins -> Deephaven -> Connections. This should bring
   up a Connections form. If so, the C# code is functioning correctly.

2. In the following steps we deliberately use nonsense connection settings
   in order to quickly trigger an error. Even thought he connection settings
   are nonsense, getting an error quickly confirms that the functionality
   of the C++ code is working.

3. Inside Connections, click the "New Connection" button. A "Credentials
   Editor" window will pop up. Inside this window, enter "con1" for the
   Connection ID, select the "Community Core" button, and enter a nonsense
   endpoint address like "abc...def"

4. Press Test Credentials. You should immediately see an error like
   "Can't get configuration constants. Error 14: DNS resolution failed for
   abc...def"

5. Enterprise users can do a similar test by selecting the Enterprise Core+
   button and putting the same nonsense address abc...def into the JSON URL
   field.
   
### By connecting to a Deephaven server

If the above tests pass, the add-in is probably installed correctly.

To test the add-in with a Deephaven server, you will need the following
information.

1. For Community Core, you will need a Connection string in the form of
   address:port. For example, 10.0.1.50:10000

2. For Enterprise Core+, you will need a JSON URL that references your
   Core+ installation. For example 
   https://10.0.1.50:8123/iris/connection.json
