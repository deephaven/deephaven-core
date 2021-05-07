"""
The main usage of this module is to configure jpy with respect to a given Java version and Python version.
The module can also be used as tool. For usage, type:

    python jpyutil.py --help

The function being invoked here is `write_config_files()` which may also be directly used from your Python code.
It will create a file 'jpyconfig.py' and/or a 'jpyconfig.properties' in the given output directory which is usually
the one in which the jpy module is installed.

To programmatically configure the Java Virtual machine, the `init_jvm()` function can be used:

    import jpyutil
    jpyutil.init_jvm(jvm_maxmem='512M', jvm_classpath=['target/test-classes'])
    # Without the former call, the following jpy import would create a JVM with default settings
    import jpy

The `init_jvm()` can also be called with 'config_file' or 'config' arguments instead. If they are omitted,
default configuration values will inferred by first trying to import the module 'jpyconfig.py' (which must
therefore be detectable by the Python sys.path). Secondly, the environment variable 'JPY_PY_CONFIG' may point to
such a Python configuration file.

"""

import sys
import sysconfig
import os
import os.path
import platform
import ctypes
import ctypes.util
import logging
import subprocess


__author__ = "Norman Fomferra (Brockmann Consult GmbH) and contributors"
__copyright__ = "Copyright 2015-2018 Brockmann Consult GmbH and contributors"
__license__ = "Apache 2.0"
__version__ = "0.10.0.dev1"


# Setup a dedicated logger for jpyutil.
# This way importing jpyutil does not interfere with logging in other modules
logger = logging.getLogger('jpyutil')
# Get log level from environment variable JPY_LOG_LEVEL. Default to INFO
log_level = os.getenv('JPY_LOG_LEVEL', 'INFO')
try:
    logger.setLevel(getattr(logging, log_level))
except AttributeError as ex:
    print('JPY_LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR or CRITICAL')
    raise ex

ch = logging.StreamHandler()
ch.setLevel(getattr(logging, log_level))
formatter = logging.Formatter('%(name)s - %(levelname)s: %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

JDK_HOME_VARS = ('JPY_JAVA_HOME', 'JPY_JDK_HOME', 'JAVA_HOME', 'JDK_HOME',)
JRE_HOME_VARS = ('JPY_JAVA_HOME', 'JPY_JDK_HOME', 'JPY_JRE_HOME', 'JAVA_HOME', 'JDK_HOME', 'JRE_HOME', 'JAVA_JRE')
JVM_LIB_NAME = 'jvm'


def _get_python_lib_name():
    try:
        abiflags = sys.abiflags
    except AttributeError:
        abiflags = ''
    version = sysconfig.get_config_var('VERSION')
    if not version:
        version = ''
    return 'python' + version + abiflags


PYTHON_64BIT = sys.maxsize > 2 ** 32
PYTHON_LIB_NAME = _get_python_lib_name()

PYTHON_LIB_DIR_CONFIG_VAR_NAMES = ('LDLIBRARYDIR', 'srcdir',
                                   'BINDIR', 'DESTLIB', 'DESTSHARED',
                                   'BINLIBDEST', 'LIBDEST', 'LIBDIR',)


def _get_unique_config_values(names):
    values = []
    for name in names:
        value = sysconfig.get_config_var(name)
        if value and not value in values:
            values.append(value)
    return values


def _add_paths_if_exists(path_list, *paths):
    for path in paths:
        if os.path.exists(path) and not path in path_list:
            path_list.append(path)
    return path_list


def _get_module_path(name, fail=False, install_path=None):
    """ Find the path to the jpy jni modules. """

    path = None
    if sys.version_info < (3, 4):
        import imp
        try:
            details = imp.find_module(name)  # this should raise an ImportError if module is not found
            path = details[1]
        except ImportError as e:
            if fail:
                raise e
    else:
        import importlib
        try:
            details = importlib.util.find_spec(name)  # this should raise an ImportError if module is not found
            if hasattr(details, 'has_location') and details.has_location:
                # we have a loadable origin - it's a package
                path = details.origin
            elif fail:
                raise ImportError("No loadable module '{}' found".format(name))
        except (ImportError, ModuleNotFoundError, AttributeError) as e:
            if fail:
                raise e

    if not path and fail:
        raise ImportError("module '{}' is not discovered or is missing a file path".format(name))

    if path is None or not install_path:
        return path
    return os.path.join(install_path, os.path.split(path)[1])


def _find_file(search_dirs, *filenames):
    for filename in filenames:
        for dir in search_dirs:
            path = os.path.normpath(os.path.join(dir, filename))
            path_exists = os.path.exists(path)
            logger.debug("Exists '%s'? %s" % (path, "yes" if path_exists else "no"))
            if path_exists:
                return path
    return None


def _get_java_api_properties(fail=False, path=None):
    jpy_config = Properties()
    jpy_config.set_property('jpy.jpyLib', _get_module_path('jpy', fail=fail, install_path=path))
    jpy_config.set_property('jpy.jdlLib', _get_module_path('jdl', fail=fail, install_path=path))
    jpy_config.set_property('jpy.pythonLib', _find_python_dll_file(fail=fail))
    jpy_config.set_property('jpy.pythonPrefix', sys.prefix)
    jpy_config.set_property('jpy.pythonExecutable', sys.executable)
    return jpy_config


def find_jdk_home_dir():
    """
    Try to detect the JDK home directory from Maven, if available, or use
    dedicated environment variables.
    :return: pathname if found, else None
    """
    for name in JDK_HOME_VARS:
        jdk_home_dir = os.environ.get(name, None)
        if jdk_home_dir \
                and os.path.exists(os.path.join(jdk_home_dir, 'include')) \
                and os.path.exists(os.path.join(jdk_home_dir, 'lib')):
            return jdk_home_dir
    logger.debug('Checking Maven for JAVA_HOME...')
    try:
        output = subprocess.check_output(['mvn', '-v'])
        if isinstance(output, bytes) and not isinstance(output, str):
            #PY3 related
            output = output.decode('utf-8')
        for part in output.split('\n'):
            if part.startswith('Java home:'):
                path = part.split(':')[1].strip()
                if path.endswith('jre'):
                    return path[0:-3]
                
    except Exception:
        # maven probably isn't installed or not on PATH
        logger.debug('Maven not found on PATH. No JAVA_HOME found.')

    return None


def find_jvm_dll_file(java_home_dir=None, fail=False):
    """
    Try to detect the JVM's shared library file.
    :param java_home_dir: The Java JRE or JDK installation directory to be used for searching.
    :return: pathname if found, else None
    """

    logger.debug("Searching for JVM shared library file")

    if java_home_dir:
        jvm_dll_path = _find_jvm_dll_file(java_home_dir)
        if jvm_dll_path:
            return jvm_dll_path

    jvm_dll_path = os.environ.get('JPY_JVM_DLL', None)
    if jvm_dll_path:
        return jvm_dll_path

    for name in JRE_HOME_VARS:
        java_home_dir = os.environ.get(name, None)
        if java_home_dir:
            jvm_dll_path = _find_jvm_dll_file(java_home_dir)
            if jvm_dll_path:
                return jvm_dll_path

    jvm_dll_path = ctypes.util.find_library(JVM_LIB_NAME)
    if jvm_dll_path:
        logger.debug("No JVM shared library file found in all search paths. Using fallback %s" % repr(jvm_dll_path))
    elif fail:
        raise RuntimeError("can't find any JVM shared library")

    return jvm_dll_path


def _get_jvm_lib_dirs(java_home_dir):
    arch = 'amd64' if PYTHON_64BIT else 'i386'
    return (os.path.join(java_home_dir, 'bin'),
            os.path.join(java_home_dir, 'bin', 'server'),
            os.path.join(java_home_dir, 'bin', 'client'),
            os.path.join(java_home_dir, 'bin', arch),
            os.path.join(java_home_dir, 'bin', arch, 'server'),
            os.path.join(java_home_dir, 'bin', arch, 'client'),
            os.path.join(java_home_dir, 'lib'),
            os.path.join(java_home_dir, 'lib', 'server'),
            os.path.join(java_home_dir, 'lib', 'client'),
            os.path.join(java_home_dir, 'lib', arch),
            os.path.join(java_home_dir, 'lib', arch, 'server'),
            os.path.join(java_home_dir, 'lib', arch, 'client'),
            )


def _get_existing_subdirs(dirs, subdirname):
    new_dirs = []
    for dir in dirs:
        new_dir = os.path.join(dir, subdirname)
        if os.path.isdir(new_dir):
            new_dirs.append(new_dir)
    return new_dirs


def _find_jvm_dll_file(java_home_dir):
    logger.debug("Searching for JVM shared library file in %s" % repr(java_home_dir))

    if not os.path.exists(java_home_dir):
        return None

    search_dirs = []
    jre_home_dir = os.path.join(java_home_dir, 'jre')
    if os.path.exists(jre_home_dir):
        search_dirs += _get_jvm_lib_dirs(jre_home_dir)
    search_dirs += _get_jvm_lib_dirs(java_home_dir)

    search_dirs = _add_paths_if_exists([], *search_dirs)

    if platform.system() == 'Windows':
        return _find_file(search_dirs, 'jvm.dll')
    elif platform.system() == 'Darwin':
        return _find_file(search_dirs, 'libjvm.dylib')

    # 'Window' and 'Darwin' did not succeed, try 'libjvm.so' on remaining platforms
    return _find_file(search_dirs, 'libjvm.so')


def _find_python_dll_file(fail=False):
    logger.debug("Searching for Python shared library file")

    #
    # Prepare list of search directories
    #

    search_dirs = [sys.prefix]

    extra_search_dirs = [sysconfig.get_config_var(name) for name in PYTHON_LIB_DIR_CONFIG_VAR_NAMES]
    for extra_dir in extra_search_dirs:
        if extra_dir and not extra_dir in search_dirs and os.path.exists(extra_dir):
            search_dirs.append(extra_dir)

    if platform.system() == 'Windows':
        extra_search_dirs = _get_existing_subdirs(search_dirs, "DLLs")
        search_dirs = extra_search_dirs + search_dirs

    multi_arch_sub_dir = sysconfig.get_config_var('multiarchsubdir')
    if multi_arch_sub_dir:
        while multi_arch_sub_dir.startswith('/'):
            multi_arch_sub_dir = multi_arch_sub_dir[1:]
        extra_search_dirs = _get_existing_subdirs(search_dirs, multi_arch_sub_dir)
        search_dirs = extra_search_dirs + search_dirs

    logger.debug("Potential Python shared library search dirs: %s" % repr(search_dirs))

    #
    # Prepare list of possible library file names
    #

    vmaj = str(sys.version_info.major)
    vmin = str(sys.version_info.minor)

    if platform.system() == 'Windows':
        versions = (vmaj + vmin, vmaj, '')
        file_names = ['python' + v + '.dll' for v in versions]
    elif platform.system() == 'Darwin':
        versions = (vmaj + "." + vmin, vmaj, '')
        file_names = ['libpython' + v + '.dylib' for v in versions] + \
                     ['libpython' + v + '.so' for v in versions]
    else:
        versions = (vmaj + "." + vmin, vmaj, '')
        file_names = ['libpython' + v + '.so' for v in versions]

    logger.debug("Potential Python shared library file names: %s" % repr(file_names))

    python_dll_path = _find_file(search_dirs, *file_names)
    if python_dll_path:
        return python_dll_path

    python_dll_path = ctypes.util.find_library(PYTHON_LIB_NAME)
    if python_dll_path:
        logger.debug(
            "No Python shared library file found in all search paths. Using fallback %s" % repr(python_dll_path))
    elif fail:
        raise RuntimeError("can't find any Python shared library")

    return python_dll_path


def _read_config(config_file):
    config = Config()
    config.load(config_file)
    return config


def _get_python_api_config(config_file=None):
    if config_file:
        # 1. Try argument, if any
        return _read_config(config_file)

    try:
        # 2. Try Python import machinery
        import jpyconfig

        return jpyconfig

    except ImportError:
        # 3. Try 'JPY_PY_CONFIG' environment variable, if any
        config_file = os.environ.get('JPY_PY_CONFIG', None)
        if config_file:
            return _read_config(config_file)

    return None


def preload_jvm_dll(jvm_dll_file=None,
                    java_home_dir=None,
                    config_file=None,
                    config=None,
                    fail=True):
    # if jvm_dll_file is unknown, try getting it from config
    if not jvm_dll_file:
        if not config:
            config = _get_python_api_config(config_file=config_file)

        if config:
            jvm_dll_file = getattr(config, 'jvm_dll', None)
            if not java_home_dir:
                java_home_dir = getattr(config, 'java_home', None)

    # if jvm_dll_file is still unknown, try searching it using java_home_dir
    if not jvm_dll_file:
        jvm_dll_file = find_jvm_dll_file(java_home_dir=java_home_dir, fail=fail)

    if jvm_dll_file:
        logger.debug('Preloading JVM shared library %s' % repr(jvm_dll_file))
        return ctypes.CDLL(jvm_dll_file, mode=ctypes.RTLD_GLOBAL)
    else:
        logger.warning('Failed to preload JVM shared library. No shared library found.')
        return None


def get_jvm_options(jvm_maxmem=None,
                    jvm_classpath=None,
                    jvm_properties=None,
                    jvm_options=None,
                    config=None):
    if config:
        if not jvm_maxmem:
            jvm_maxmem = getattr(config, 'jvm_maxmem', None)
        if not jvm_classpath:
            jvm_classpath = getattr(config, 'jvm_classpath', None)
        if not jvm_properties:
            jvm_properties = getattr(config, 'jvm_properties', None)
        if not jvm_options:
            jvm_options = getattr(config, 'jvm_options', None)

    jvm_cp = None
    if jvm_classpath and len(jvm_classpath) > 0:
        jvm_cp = os.pathsep.join(jvm_classpath)
    if not jvm_cp:
        jvm_cp = os.environ.get('JPY_JVM_CLASSPATH', None)

    if not jvm_maxmem:
        jvm_maxmem = os.environ.get('JPY_JVM_MAXMEM', None)

    java_api_properties = _get_java_api_properties().values
    if jvm_properties:
        # Overwrite jpy_config
        jvm_properties = dict(list(java_api_properties.items()) + list(jvm_properties.items()))
    else:
        jvm_properties = java_api_properties

    options = []
    if jvm_maxmem:
        options.append('-Xmx' + jvm_maxmem)
    if jvm_cp:
        options.append('-Djava.class.path=' + jvm_cp)
    if jvm_properties:
        for key in jvm_properties:
            value = jvm_properties[key]
            options.append('-D' + key + '=' + value)
    if jvm_options:
        options += jvm_options

    return options


def init_jvm(java_home=None,
             jvm_dll=None,
             jvm_maxmem=None,
             jvm_classpath=None,
             jvm_properties=None,
             jvm_options=None,
             config_file=None,
             config=None):
    """
    Creates a configured Java virtual machine which will be used by jpy.

    :param java_home: The Java JRE or JDK home directory used to search JVM shared library, if 'jvm_dll' is omitted.
    :param jvm_dll: The JVM shared library file. My be inferred from 'java_home'.
    :param jvm_maxmem: The JVM maximum heap space, e.g. '400M', '8G'. Refer to the java executable '-Xmx' option.
    :param jvm_classpath: The JVM search paths for Java class files. Separated by colons (Unix) or semicolons
                          (Windows). Refer to the java executable '-cp' option.
    :param jvm_properties: A dictionary of key -> value pairs passed to the JVM as Java system properties.
                        Refer to the java executable '-D' option.
    :param jvm_options: A list of extra options for the JVM. Refer to the java executable options.
    :param config_file: Extra configuration file (e.g. 'jpyconfig.py') to be loaded if 'config' parameter is omitted.
    :param config: An optional default configuration object providing default attributes
                   for the 'jvm_maxmem', 'jvm_classpath', 'jvm_properties', 'jvm_options' parameters.
    :return: a tuple (cdll, actual_jvm_options) on success, None otherwise.
    """
    if not config:
        config = _get_python_api_config(config_file=config_file)

    cdll = preload_jvm_dll(jvm_dll_file=jvm_dll,
                           java_home_dir=java_home,
                           config_file=config_file,
                           config=config,
                           fail=False)

    import jpy

    if not jpy.has_jvm():
        jvm_options = get_jvm_options(jvm_maxmem=jvm_maxmem,
                                      jvm_classpath=jvm_classpath,
                                      jvm_properties=jvm_properties,
                                      jvm_options=jvm_options,
                                      config=config)
        logger.debug('Creating JVM with options %s' % repr(jvm_options))
        jpy.create_jvm(options=jvm_options)
    else:
        jvm_options = None

    # print('jvm_dll =', jvm_dll)
    # print('jvm_options =', jvm_options)
    return cdll, jvm_options


class Config:
    def load(self, path):
        """
        Read Python file from 'path', execute it and return object that stores all variables of the
        Python code as attributes.
        :param path:
        :return:
        """
        with open(path) as f:
            code = f.read()
            exec(code, {}, self.__dict__)


class Properties:
    def __init__(self, values=None):
        if values:
            self.keys = values.keys()
            self.values = values.copy()
        else:
            self.keys = []
            self.values = {}

    def set_property(self, key, value):
        if value:
            if not key in self.keys:
                self.keys.append(key)
            self.values[key] = value
        else:
            if key in self.keys:
                self.keys.remove(key)
                self.values.pop(key)

    def get_property(self, key, default_value=None):
        return self.values[key] if key in self.values else default_value

    def store(self, path, comments=()):
        with open(path, 'w') as f:
            for comment in comments:
                f.write('# ' + str(comment).replace('\\', '\\\\') + '\n')
            for key in self.keys:
                value = self.get_property(key)
                if value:
                    f.write(str(key) + ' = ' + str(value).replace('\\', '\\\\') + '\n')
                else:
                    f.write(str(key) + ' =\n')

    def load(self, path):
        self.__init__()
        with open(path) as f:
            lines = f.readlines()
            for line in lines:
                if line and len(line) > 0 and not line.startswith('#'):
                    tokens = line.split('=')
                    if len(tokens) == 2:
                        self.set_property(tokens[0].strip(), tokens[1].strip().replace('\\\\', '\\'))
                    else:
                        raise ValueError('illegal Java properties format ' + line)


def _execute_python_scripts(scripts, **kwargs):
    import subprocess

    failures = 0
    for script in scripts:
        exit_code = subprocess.call([sys.executable, script], **kwargs)
        if exit_code:
            failures += 1
    return failures


def write_config_files(out_dir='.',
                       java_home_dir=None,
                       jvm_dll_file=None,
                       install_dir=None,
                       req_java_api_conf=True,
                       req_py_api_conf=True):
    """
    Writes the jpy configuration files for Java and/or Python.

    :param out_dir: output directory, must exist
    :param java_home_dir: optional home directory of the Java JRE or JDK installation
    :param jvm_dll_file: optional file to JVM shared library file
    :param install_dir: optional path to where to searfh for modules
    :param req_java_api_conf: whether to write the jpy configuration file 'jpyconfig.properties' for Java
    :param req_py_api_conf: whether to write the jpy configuration file 'jpyconfig.py' for Python
    :return: zero on success, otherwise an error code
    """
    import datetime

    retcode = 0

    tool_name = os.path.basename(__file__)

    py_api_config_basename = 'jpyconfig.py'
    java_api_config_basename = 'jpyconfig.properties'

    if not jvm_dll_file:
        jvm_dll_file = find_jvm_dll_file(java_home_dir=java_home_dir)
    if jvm_dll_file:
        py_api_config_file = os.path.join(out_dir, py_api_config_basename)
        try:
            with open(py_api_config_file, 'w') as f:
                f.write("# Created by '%s' tool on %s\n" % (tool_name, str(datetime.datetime.now())))
                f.write(
                    "# This file is read by the 'jpyutil' module in order to load and configure the JVM from Python\n")
                if java_home_dir:
                    f.write('java_home = %s\n' % repr(java_home_dir))
                f.write('jvm_dll = %s\n' % repr(jvm_dll_file))
                f.write('jvm_maxmem = None\n')
                f.write('jvm_classpath = []\n')
                f.write('jvm_properties = {}\n')
                f.write('jvm_options = []\n')
            logger.info("jpy Python API configuration written to '%s'" % py_api_config_file)
        except Exception:
            logger.exception("Error while writing Python API configuration")
            if req_py_api_conf:
                retcode = 1
    else:
        logger.error("Can't determine any JVM shared library")
        if req_py_api_conf:
            retcode = 2

    try:
        java_api_config_file = os.path.join(out_dir, java_api_config_basename)
        java_api_properties = _get_java_api_properties(fail=req_java_api_conf, path=install_dir)
        java_api_properties.store(java_api_config_file, comments=[
            "Created by '%s' tool on %s" % (tool_name, str(datetime.datetime.now())),
            "This file is read by the jpy Java API (org.jpy.PyLib class) in order to find shared libraries"])
        logger.info("jpy Java API configuration written to '%s'" % java_api_config_file)
    except Exception:
        logger.exception("Error while writing Java API configuration")
        if req_java_api_conf:
            retcode = 3

    return retcode


def _main():
    import argparse

    out_default = os.path.dirname(os.path.abspath(__file__))

    parser = argparse.ArgumentParser(description='Generate configuration files for the jpy Python API (jpyconfig.py)\n'
                                                 'and the jpy Java API (jpyconfig.properties).')
    parser.add_argument("-o", "--out", action='store', default=out_default,
                        help="Output directory for the generated configuration files. Defaults to " + out_default)
    parser.add_argument("--java_home", action='store', default=None, help="Java JDK or JRE home directory. Can also be "
                                                                          "set using one of the environment variables "
                                                                          + " | ".join(JRE_HOME_VARS) + ".")
    parser.add_argument("--jvm_dll", action='store', default=None, help="Java JVM shared library file. Usually inferred"
                                                                        " from java_home option. Can also be set "
                                                                        "using environment variable JPY_JVM_DLL.")
    parser.add_argument("--log_file", action='store', default=None, help="Optional log file.")
    parser.add_argument("--log_level", action='store', default='INFO',
                        help="Possible values: DEBUG, INFO, WARNING, ERROR. Default is INFO.")
    parser.add_argument("-j", "--req_java", action='store_true', default=False,
                        help="Require that Java API configuration succeeds.")
    parser.add_argument("-p", "--req_py", action='store_true', default=False,
                        help="Require that Python API configuration succeeds.")
    parser.add_argument("--install_dir", action='store', default=None, help="Optional. Used during pip install of JPY")
    args = parser.parse_args()

    log_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(log_level, int):
        raise ValueError('Invalid log level: %s' % log_level)

    log_format = '%(levelname)s: %(message)s'
    log_file = args.log_file
    if log_file:
        logging.basicConfig(format=log_format, level=log_level, filename=log_file, filemode='w')
    else:
        logging.basicConfig(format=log_format, level=log_level)

    try:
        retcode = write_config_files(out_dir=args.out,
                                     java_home_dir=args.java_home,
                                     jvm_dll_file=args.jvm_dll,
                                     req_java_api_conf=args.req_java,
                                     req_py_api_conf=args.req_py,
                                     install_dir=args.install_dir)
    except:
        logger.exception("Configuration failed")
        retcode = 100

    if retcode == 0:
        logger.info("Configuration completed successfully")

    exit(retcode)


if __name__ == '__main__':
    _main()
