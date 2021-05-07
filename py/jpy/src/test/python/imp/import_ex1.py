"""
Testing Python import machinery with the goal to let users import Java class like this:
    from jpy.java.io import File
or
    from jpy import java.io.File as File
"""

__author__ = 'Norman'

import sys
import importlib.abc


class JavaClass:
    def __init__(self, fullname):
        self.fullname = fullname
    
    def __repr__(self):
        return "JavaClass('" + self.fullname + "')" 

    def __str__(self):
        return self.fullname 


class JavaPackageImporter(importlib.abc.MetaPathFinder):

    def __init__(self, file):
        self.packages = set()        
        #with open(file, "r") as f:
        #    for line in f:
        #        package = line.strip()
        #        self.register_package(package)
        #print(self.packages)


    def register_package(self, fullname):
        package = fullname
        while package:
            self.packages.add(package)
            dot_pos = package.rfind('.')
            if dot_pos > 0:
                package = package[:dot_pos]
            else: 
                package = None

                
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
        package, name = self.get_package(fullname)

        print('creating module "' + fullname + '"')
        module = type(sys)(fullname)
        module.__loader__ = self
        module.__path__ = None
        module.__package__ = package
        return module


    def find_module(self, fullname, path):
        print('find_module(fullname="' + str(fullname) + '", path="' + str(path) + '")')
        if fullname in self.packages:
            return self
        
        package, name = self.get_package(fullname)
        if package in self.packages:
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
            if fullname in self.packages:
                print('known package: ' + fullname)
                module = self.new_module(fullname)
                sys.modules[fullname] = module
            else:
                print('not a known package: ' + fullname)
                package, class_name = self.get_package(fullname)
                module = sys.modules[package]
                if module is None:
                    return None
                module = self.load_java_class(module, fullname)
        return module


        
jpi = JavaPackageImporter('packages-jre7.txt')
#jpi.register_package('org.esa.beam.framework.datamodel')
#jpi.register_package('org.esa.beam.framework.dataio')
#jpi.register_package('org.esa.beam.framework.gpf')

sys.meta_path += [jpi]

#import jpy
import bibo
import riser
#from jpy import Riser as Riser


import numpy
from numpy import array


#import java.io
#import java.lang
#import java.util
#import numpy
#import java.wraaaw
#from java.util import String
#from java.io import File

#s = String('Hello')
#print(f)

#f = File('x')
#print(f)


#import os.path
#
#print(os.__name__)
#print(os.__package__)
#print(os.__file__)
#
#print(dir(java.io))
#
#
#print(os.__name__)
#print(os.__package__)
#print(os.__file__)
#print(list(os.__path__))
#print(os.path.__name__)
#print(os.path.__package__)
#print(os.path.__file__)
#print(list(os.path.__path__))
#
#print('java.io:', dir(java.io))

