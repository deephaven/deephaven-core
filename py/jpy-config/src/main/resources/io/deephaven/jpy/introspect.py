import sys
import jpyutil

jpy_properties = {
    'jpy.pythonPrefix': sys.prefix,
    'jpy.programName': sys.executable,
    'jpy.pythonLib': jpyutil._find_python_dll_file(fail=True),
    'jpy.jpyLib': jpyutil._get_module_path('jpy', fail=True),
    'jpy.jdlLib': jpyutil._get_module_path('jdl', fail=True)
}

jpy_properties_format = '\n'.join([ '{}={}'.format(key, value) for key, value in jpy_properties.items() ])

print(jpy_properties_format)
