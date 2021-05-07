#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

# add JDK to path (otherwise jnius gives DLL load error)
import os
os.environ['PATH'] = os.environ['PATH'] + ";C:\\Program Files\\Java\jdk1.8.0_72\\jre\\bin\\server"
os.environ['PATH'] = os.environ['PATH'] + ";C:\\Program Files\\Java\jdk1.8.0_60\\jre\\bin\\server"
print(os.environ['PATH'])


import jpyutil
jpyutil.init_jvm()
# jpyutil.init_jvm(jvm_maxmem='512M', jvm_classpath=['target/test-classes'])

import jpy

Stack = jpy.get_type('java.util.Stack')
stack = Stack()
stack.push('hello')
stack.push('world')

print(stack.pop()) # --> 'world'
print(stack.pop()) # --> 'hello'

print(stack.getClass().getName())

