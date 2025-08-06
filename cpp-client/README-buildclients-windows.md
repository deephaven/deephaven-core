# Building the Deephaven clients from source on Windows 10 / 11

These instructions describe how to build the Deephaven clients from source on Windows 10 / 11.

To begin, first read the Prerequisites section. Then follow the instructions for the client you
want to build. Note that some clients depend on other clients being built first. These
dependencies will be explained in the section for each client.

## Prerequisites

### Build machine specifications

* Disk space: at least 150G
* Intel/AMD CPUs (this is our only tested configuration)
* Cores: 2 or 4 cores is fine, *except* for the initial C++ build (without a vcpkg cache).
  If you are doing a C++ build for the first time on a fresh machine, 16 cores is preferable
  in order to populate the vcpkg cache. 

### Software prerequisites

1. Install Visual Studio 2022 Community Edition (or Professional, or Enterprise)
   from here:

   https://visualstudio.microsoft.com/downloads/

   When the installer runs, select the following workloads:
   * "Desktop development with C++"
   * "Python development"

2. Install Python 3.10 from the Microsoft Store

3. Install Java JDK 21 for Windows from

   https://www.oracle.com/java/technologies/downloads/

4. Use your preferred version of git, or install Git from here:

   https://git-scm.com/download/win

   When running Setup, select the option "Git from the command line and also
   from 3rd-party software". This allows you to use git from the Windows command
   prompt.

### Command Prompt, venv, and Environment variables

1. Start a x64 Native Tools Command Prompt. Note this is NOT the regular Command Prompt,
   nor the Visual Studio Developer Command Prompt. 

* `Start -> V -> Visual Studio 2022 -> x64 Native Tools Command Prompt for VS 2022`

2. (Optional) override environment variables

* set `DHSRC=... # Directory for Deephaven Core checkouts and builds`
* set `DHINSTALL=... # Directory where C++ (Core) and C++ (Core+) is installed`


### Dependency Matrix

Some of the clients require others to be built first. This is the client dependency matrix.

| Client (Deephaven version)  | Depends On                |
|-----------------------------|---------------------------|
| C++ (Core)                  | ---                       |
| C++ (Core+)                 | C++ (Core)                |
| python (Core) [non-ticking] | ---                       |
| python-ticking (Core)       | C++ (Core), python (Core) |
| python-ticking (Core)       | C++ (Core), python (Core) |
| R (Core)                    | C++ (Core)                |
| R (Core+)                   | C++ (Core), C++ (Core+)   |

### Build script

Download the build script:

```
cd %HOMEDRIVE%%HOMEPATH%
curl -O https://raw.githubusercontent.com/deephaven/deephaven-core/refs/heads/main/cpp-client/build-windows-clients.bat`
  * NOTE TO REVIEWERS USE THIS FOR NOW:
  * `curl -O https://raw.githubusercontent.com/kosak/deephaven-core/refs/heads/kosak_consolidated-client-build-readmes/cpp-client/build-windows-clients.bat`
```

## Building the clients

To build the clients, first consult this table to determine the keyword of the client
you want to build. You can specify more than one keyword.

| Client (Deephaven version)    | Keyword                            |
|-------------------------------|------------------------------------|
| C++ (Core)                    | cpp-core-client                    |
| C++ (Core+)                   | [TODO]                             |
| python (Core) [non-ticking]   | python-core-static-client          |
| python-ticking (Core)         | python-core-ticking-client         |
| R (Core)                      | (not implemented yet for Windows)  |
| R (Core+)                     | (not implemented yet for Windows)  |

Then run these commands

```
cd %HOMEDRIVE%%HOMEPATH%
.\build-windows-clients [keyword or keywords selected above]
```

For example, to build the C++ Core client, the Python core static client, and the Python core ticking
client, run this command

```
cd %HOMEDRIVE%%HOMEPATH%
.\build-windows-clients cpp-core-client python-core-static-client python-core-ticking-client
```
