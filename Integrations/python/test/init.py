import sys
import os
import argparse
import jpy

# Default for arguments
DEFAULT_OUTPUT = None

DEFAULT_DEVROOT = os.environ.get('DEEPHAVEN_DEVROOT', None)

DEFAULT_WORKSPACE = os.environ.get('DEEPHAVEN_WORKSPACE', None)

DEFAULT_PROPFILE = os.environ.get('DEEPHAVEN_PROPFILE', None)

DEFAULT_CLASSPATH = os.environ.get('DEEPHAVEN_CLASSPATH', None)


# Set up our arg parser mumbo jumbo
initParser = argparse.ArgumentParser(add_help=False)

# use to insert some modules at the head of the python path (override system module for testing purposes)
initParser.add_argument(
    "--insert_python_head",
    default=None,
    help="Element to insert at the head of the python path. "
         "This is really intended to enable you to use the contents of the python "
         "presently in your Deephaven branch as the deephaven python modules.")

# these detail jvm initialization options
initParser.add_argument(
    "-d", "--devroot",
    default=DEFAULT_DEVROOT,
    help="path for `devroot` passed through for jvm initialization")

initParser.add_argument(
    "-w", "--workspace",
    default=DEFAULT_WORKSPACE,
    help="path for `workspace` passed through for jvm initialization")

initParser.add_argument(
    "-p", "--propfile",
    default=DEFAULT_PROPFILE,
    help="path to Deephaven prop file, passed in to jvm initialization")

initParser.add_argument(
    "-cp", "--classpath",
    default=DEFAULT_CLASSPATH,
    help="jvm classpath; use `environment 'DEEPHAVEN_CLASSPATH', configurations.someConfig.asPath`")


def main(args):
    if args.insert_python_head is not None:
        # short circuit which module is being used, for testing
        # intended to allow short circuiting of Deephaven python module
        sys.path.insert(0, args.insert_python_head)

    from deephaven_legacy import start_jvm

    if not jpy.has_jvm():
        # we will try to initialize the jvm
        kwargs = {
            'workspace': args.workspace,
            'devroot': args.devroot,
            'verbose': False,
            'propfile': args.propfile,
            'java_home': os.environ.get('JDK_HOME', None),
            'jvm_properties': {},
            'jvm_options': {'-Djava.awt.headless=true',
                            '-Xms1g',
                            '-Xmn512m',
                            # '-verbose:gc', '-XX:+PrintGCDetails',
            },
            'jvm_maxmem': '1g'
        }
        if args.classpath:
            kwargs['jvm_classpath'] = args.classpath
            kwargs['skip_default_classpath'] = True
            # kwargs['verbose'] = True
        # initialize the jvm
        start_jvm(**kwargs)


if __name__ == '__main__':
    args = initParser.parse_args()
    main(args)
