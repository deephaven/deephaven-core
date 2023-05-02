#!/usr/bin/python

import jpy
import os
import sys
from deephaven_internal.stream import TeeStream

# Set stdin to /dev/null to prevent functions (like help()) that attempt to read from stdin from hanging python
# execution from within Java.
os.dup2(os.open("/dev/null", os.O_RDONLY), 0)

jpy.VerboseExceptions.enabled = True
# If you want jpy to tell you about all that it is doing, change this
# jpy.diag.flags = jpy.diag.F_ALL

j_sys = jpy.get_type('java.lang.System')
sys.stdout = TeeStream.redirect(sys.stdout, j_sys.out)
sys.stderr = TeeStream.redirect(sys.stderr, j_sys.err)
