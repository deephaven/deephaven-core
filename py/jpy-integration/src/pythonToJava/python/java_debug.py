if __name__ != '__main__':
  quit()

import jpyutil
jpyutil.preload_jvm_dll()
import jpy

print(jpy.has_jvm())
print(jpyutil.init_jvm())
print(jpy.has_jvm())

System = jpy.get_type('java.lang.System')
File = jpy.get_type('java.io.File')

classpath = System.getProperty('java.class.path')
separator = File.pathSeparator
classpathEntries = classpath.split(separator)
for entry in classpathEntries:
  print(entry)