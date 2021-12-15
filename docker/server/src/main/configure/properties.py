#!/usr/bin/env python3

import sys
import jpyutil

properties = {
    'jpy.pythonPrefix': sys.prefix,
    'jpy.programName': sys.executable,
    'jpy.pythonLib': jpyutil._find_python_dll_file(fail=True),
    'jpy.jpyLib': jpyutil._get_module_path('jpy', fail=True),
    'jpy.jdlLib': jpyutil._get_module_path('jdl', fail=True)
}

for key, value in properties.items():
    print(f'{key}={value}')
