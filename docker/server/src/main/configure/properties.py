#!/usr/bin/env python3

import sys
import jpyutil

print("jpy.pythonPrefix={}".format(sys.prefix))
print("jpy.pythonExecutable={}".format(sys.executable))
print("jpy.pythonLib={}".format(jpyutil._find_python_dll_file(fail=True)))
print("jpy.jpyLib={}".format(jpyutil._get_module_path('jpy', fail=True)))
print("jpy.jdlLib={}".format(jpyutil._get_module_path('jdl', fail=True)))
