"""
Testing Python import machinery with the goal to let users import Java class like this:
    from jpy.java.io import File
or
    from jpy import java.io.File as File
"""

__author__ = 'Norman'

import sys
import importlib.abc

ModuleType = type(sys)
yyyy = ModuleType('yyyy')


class JavaClass:
    def __init__(self, fullname):
        self.fullname = fullname
    
    def __repr__(self):
        return "JavaClass('" + self.fullname + "')" 

    def __str__(self):
        return self.fullname 


class JavaPackageImporter(importlib.abc.MetaPathFinder):
        
    def get_package(self, fullname):
        dot_pos = fullname.rfind('.')
        if dot_pos > 0:        
            package = fullname[:dot_pos]
            name = fullname[dot_pos + 1:]
        else:
            package = ''
            name = fullname
        return (package, name)

    def load_java_class(self, module, fullname):
        print('try loading java class "' + fullname + '"...')
        package, name = self.get_package(fullname)
        #module.__dict__[name] = JavaClass(fullname)
        #return JavaClass(fullname)
        return None
    
    def new_module(self, fullname):
        #package, name = self.get_package(fullname)

        print('creating module "' + fullname + '"')
        module = type(sys)(fullname)
        module.__loader__ = self
        module.__path__ = None
        #module.__package__ = package
        return module


    def find_module(self, fullname, path):
        print('find_module(fullname="' + str(fullname) + '", path="' + str(path) + '")')
        
        if fullname == 'yyyy' or fullname.startswith('yyyy.'):
            return self
            
        return None

        
    def load_module(self, fullname):
        print('load_module(fullname="' + str(fullname) + '")')
        is_reload = fullname in sys.modules
        if is_reload:
            module = sys.modules[fullname]
            print('module found!')
            # Now, check what to do next...?
        else:
            print('new module: ' + fullname)
            module = self.new_module(fullname)
            sys.modules[fullname] = module
        return module

        
jpi = JavaPackageImporter()
sys.meta_path += [jpi]

import yyyy.bibo
import yyyy.riser.bibo
import yyyy.bibo.riser
from yyyy.bibo import Riser as Riser


import numpy
from numpy import array
