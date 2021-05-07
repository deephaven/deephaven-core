:: Windows SDK 7.1 settings
SET SDK_SETENV=C:\Program Files\Microsoft SDKs\Windows\v7.1\bin\setenv
SET VS90COMNTOOLS=%VS100COMNTOOLS%
SET DISTUTILS_USE_SDK=1

:: VS 14 settings (with Windows 10)
::SET SDK_SETENV=C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC
::SET VS100COMNTOOLS=C:\Program Files (x86)\Microsoft Visual Studio 14.0\Common7\Tools\


SET JDK32_HOME=C:\Program Files (x86)\Java\jdk1.8.0_112
SET JDK64_HOME=C:\Program Files\Java\jdk1.8.0_112

SET PY27_64=D:\jpy\Python27-amd64
SET PY33_64=D:\jpy\Python33-amd64
SET PY34_64=D:\jpy\Python34-amd64
SET PY35_64=D:\jpy\Python35-amd64
SET PY27_32=D:\jpy\Python27
SET PY33_32=D:\jpy\Python33
SET PY34_32=D:\jpy\Python34
SET PY35_32=D:\jpy\Python35

SET OLD_PYTHONHOME=%PYTHONHOME%
SET PYTHONHOME=

IF NOT EXIST "%JDK64_HOME%" GOTO Build_32
SET JAVA_HOME=%JDK64_HOME%
CALL "%SDK_SETENV%" /x64 /release

SET PYTHONHOME=%PY27_64%
IF NOT EXIST "%PYTHONHOME%" GOTO Build_PY33_64
ECHO Starting build using "%PYTHONHOME%" and "%JAVA_HOME%"
"%PYTHONHOME%\python.exe" setup.py --maven bdist_wheel


:Build_PY33_64
SET PYTHONHOME=%PY33_64%
IF NOT EXIST "%PYTHONHOME%" GOTO Build_PY34_64
ECHO Starting build using "%PYTHONHOME%" and "%JAVA_HOME%"
"%PYTHONHOME%\python.exe" setup.py --maven bdist_wheel

:Build_PY34_64
SET PYTHONHOME=%PY34_64%
IF NOT EXIST "%PYTHONHOME%" GOTO Build_32
ECHO Starting build using "%PYTHONHOME%" and "%JAVA_HOME%"
"%PYTHONHOME%\python.exe" setup.py --maven bdist_wheel

:Build_PY35_64
SET PYTHONHOME=%PY35_64%
IF NOT EXIST "%PYTHONHOME%" GOTO Build_32
ECHO Starting build using "%PYTHONHOME%" and "%JAVA_HOME%"
"%PYTHONHOME%\python.exe" setup.py --maven bdist_wheel

:Build_32

IF NOT EXIST "%JDK32_HOME%" GOTO Build_End
SET JAVA_HOME=%JDK32_HOME%
CALL "%SDK_SETENV%" /x86 /release

SET PYTHONHOME=%PY27_32%
IF NOT EXIST "%PYTHONHOME%" GOTO Build_PY33_32
ECHO Starting build using "%PYTHONHOME%" and "%JAVA_HOME%"
"%PYTHONHOME%\python.exe" setup.py --maven bdist_wheel

:Build_PY33_32
SET PYTHONHOME=%PY33_32%
IF NOT EXIST "%PYTHONHOME%" GOTO Build_PY34_32
ECHO Starting build using "%PYTHONHOME%" and "%JAVA_HOME%"
"%PYTHONHOME%\python.exe" setup.py --maven bdist_wheel

:Build_PY34_32
SET PYTHONHOME=%PY34_32%
IF NOT EXIST "%PYTHONHOME%" GOTO Build_End
ECHO Starting build using "%PYTHONHOME%" and "%JAVA_HOME%"
"%PYTHONHOME%\python.exe" setup.py --maven bdist_wheel

:Build_PY35_32
SET PYTHONHOME=%PY35_32%
IF NOT EXIST "%PYTHONHOME%" GOTO Build_End
ECHO Starting build using "%PYTHONHOME%" and "%JAVA_HOME%"
"%PYTHONHOME%\python.exe" setup.py --maven bdist_wheel

:Build_End
IF EXIST "%PYTHONHOME%" "%PYTHONHOME%\python.exe" setup.py sdist
SET PYTHONHOME=%OLD_PYTHONHOME%
