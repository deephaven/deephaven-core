#!/usr/bin/python

import jpy
import os

# Set stdin to /dev/null to prevent functions (like help()) that attempt to read from stdin from hanging the worker.
os.dup2(os.open("/dev/null", os.O_RDONLY), 0)

jpy.VerboseExceptions.enabled = True
# If you want jpy to tell you about all that it is doing, change this
# jpy.diag.flags = jpy.diag.F_ALL
