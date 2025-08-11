REM TODO change to @echo off
@echo on

REM NOTE TO DEVELOPERS: when editing this file, please preserve the CRLF
REM line ending format. Otherwise there is a behavior that can trigger under
REM very specific circumstances where labels that are a CALL target cannot
REM be found. https://www.dostips.com/forum/viewtopic.php?t=8988#p58888

pushd .
call :PROCESS_COMMAND_LINE %*
REM get here on success or failure
popd
exit /b

REM ============================================================================
:PROCESS_COMMAND_LINE

for %%a in (%*) do (
  if /I "%%~a"=="cpp-core" (
    call :BUILD_CPP_CORE || exit /b
  ) else if /I "%%~a"=="python-core-static" (
    call :BUILD_PYTHON_STATIC_CORE || exit /b
  ) else if /I "%%~a"=="python-core-ticking" (
    call :BUILD_PYTHON_TICKING_CORE || exit /b
  ) else (
    echo argument "%%a" is unrecognized
    exit /b 1
  )
)

exit /b 0

REM ============================================================================
:BUILD_CPP_CORE
REM Performance note
REM With an unpopulated vcpkg cache, this script takes a long time to build
REM the dependent packages (could take hours on a slow machine).
REM Once the vcpkg cache is populated, it is fast enough (maybe 5-10 minutes).
REM If this machine is running in the cloud, one reasonable strategy is to
REM first configure a machine with a large number of cores (16+) and run this.\
REM script once to populate the vcpkg cache. Assuming the vcpkg persists
REM between runs, subsequent runs can work with a smaller number of cores.

echo *** SETTING VARIABLES
call :SET_COMMON_VARIABLES || exit /b

echo *** CLONING NEEDED REPOSITORIES ***
call :CLONE_DHCORE_REPO || exit /b
call :CLONE_VCPKG_REPO || exit /b

echo *** BOOTSTRAPPING VCPKG ***
set VCPKG_ROOT=%DHSRC%\vcpkg
cd /d %VCPKG_ROOT% || exit /b
call .\bootstrap-vcpkg.bat || exit /b

echo *** BUILDING DEPENDENT LIBRARIES ***
cd /d %DHSRC%\deephaven-core\cpp-client\deephaven || exit /b
%VCPKG_ROOT%\vcpkg.exe install --triplet x64-windows || exit /b

echo *** CONFIGURING DEEPHAVEN BUILD ***
cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=%VCPKG_ROOT%/scripts/buildsystems/vcpkg.cmake -DCMAKE_INSTALL_PREFIX=%DHINSTALL% -DX_VCPKG_APPLOCAL_DEPS_INSTALL=ON || exit /b

echo *** BUILDING C++ CLIENT ***
cmake --build build --config RelWithDebInfo --target install -- /p:CL_MPCount=16 -m:1 || exit /b

exit /b 0

REM ============================================================================
:BUILD_PYTHON_STATIC_CORE

echo *** SETTING VARIABLES
call :SET_COMMON_VARIABLES || exit /b

echo *** CLONING NEEDED REPOSITORIES ***
call :CLONE_DHCORE_REPO || exit /b

echo *** SETTING DEEPHAVEN_VERSION ***
call :SET_DEEPHAVEN_VERSION || exit /b

echo *** CREATING AND ACTIVATING PYTHON VENV ***
call :CREATE_AND_ACTIVATE_PYTHON_VENV || exit /b

echo *** INSTALLING PYTHON PACKAGES
pip3 install wheel || exit /b

echo *** INSTALLING REQUIREMENTS
cd /d "%DHSRC%/deephaven-core\py\client" || exit /b
pip3 install -r requirements-dev.txt || exit /b

echo *** RUNNING SETUP
python setup.py bdist_wheel || exit /b

echo *** INSTALLING PYDEEPHAVEN-STATIC WHEEL

SET DEEPHAVEN_WHEEL_FILE=
FOR %%f IN (".\dist\*.whl") DO (
  SET DEEPHAVEN_WHEEL_FILE=%%f
)

pip3 install --force --no-deps %DEEPHAVEN_WHEEL_FILE% || exit /b
exit /b 0

REM ============================================================================
:BUILD_PYTHON_TICKING_CORE

echo *** SETTING VARIABLES
call :SET_COMMON_VARIABLES || exit /b

echo *** CLONING NEEDED REPOSITORIES ***
call :CLONE_DHCORE_REPO || exit /b

echo *** SETTING DEEPHAVEN_VERSION ***
call :SET_DEEPHAVEN_VERSION || exit /b

echo *** CREATING AND ACTIVATING PYTHON VENV ***
call :CREATE_AND_ACTIVATE_PYTHON_VENV || exit /b

echo *** INSTALLING PYTHON PACKAGES
pip3 install cython || exit /b

echo *** BUILDING PYDEEPHAVEN_TICKING ***
cd /d %DHSRC%\deephaven-core\py\client-ticking || exit /b

python setup.py build_ext -i || exit /b

echo *** BUILDING WHEEL ***
python setup.py bdist_wheel || exit /b

echo *** INSTALLING PYDEEPHAVEN-TICKING WHEEL

FOR %%f IN (".\dist\*.whl") DO (
  SET DEEPHAVEN_TICKING_WHEEL_FILE=%%f
)

pip3 install --force --no-deps %DEEPHAVEN_TICKING_WHEEL_FILE% || exit /b

exit /b 0

REM ================================================================
:SET_COMMON_VARIABLES
if not defined DHSRC (
  set DHSRC=%HOMEDRIVE%%HOMEPATH%\deephaven-clients\dhsrc
)
if not defined DHINSTALL (
  set DHINSTALL=%HOMEDRIVE%%HOMEPATH%\deephaven-clients\dhinstall
)
exit /b

REM ================================================================
:CLONE_DHCORE_REPO

if exist "%DHSRC%\deephaven-core" (
  echo dhcore repo already exists, continuing...
  exit /b 0
)

if not exist "%DHSRC%" (
  mkdir %DHSRC% || exit /b
)

cd /d %DHSRC% || exit /b
REM work around Windows long path issue, until deephaven-core repo is fixed
git clone --no-checkout -b main --depth 1 https://github.com/deephaven/deephaven-core.git || exit /b
cd %DHSRC%/deephaven-core
git config core.longpaths true || exit /b
git checkout main || exit /b

exit /b 0

REM ================================================================
:CLONE_VCPKG_REPO

if exist "%DHSRC%\vcpkg" (
  echo vcpkg repo already exists, continuing...
  exit /b 0
)

if not exist "%DHSRC%" (
  mkdir %DHSRC% || exit /b
)

cd /d %DHSRC% || exit /b
git clone --depth 1 https://github.com/microsoft/vcpkg.git || exit /b

exit /b 0

REM ================================================================
:CREATE_AND_ACTIVATE_PYTHON_VENV
if not exist "%DHSRC%\cython-venv" (
  python3 -m venv "%DHSRC%\cython-venv" || exit /b
)

call "%DHSRC%\cython-venv\Scripts\activate" || exit /b

exit /b 0

REM ================================================================
:SET_DEEPHAVEN_VERSION

if defined DEEPHAVEN_VERSION (
  echo DEEPHAVEN_VERSION is already set to %DEEPHAVEN_VERSION%. Continuing...
  exit /b 0
)

pushd "%DHSRC%\deephaven-core"
FOR /F "tokens=*" %%a IN ('.\gradlew :printVersion -q') DO (
  set DEEPHAVEN_VERSION=%%a
)
popd

if not defined DEEPHAVEN_VERSION (
  echo DEEPHAVEN_VERSION is not defined
  exit /b 1
)

echo DEEPHAVEN_VERSION is %DEEPHAVEN_VERSION%
exit /b 0
