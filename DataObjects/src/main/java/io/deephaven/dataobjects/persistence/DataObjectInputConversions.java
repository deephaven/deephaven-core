/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.dataobjects.persistence;

import io.deephaven.base.verify.Require;

import java.io.*;
import java.util.HashMap;

/**
 * This class is a place to put code that repairs old caches when the classes in them evolve in
 * ways that the ADO schema evolution mechanism can't handle.
 */
public class DataObjectInputConversions {

    private static final HashMap<String, DataObjectInputStream.ObjectConverter> converters = new HashMap<String, DataObjectInputStream.ObjectConverter>();

    public static DataObjectInputStream.ObjectConverter getObjectConverter(String fullClassName) {
        return converters.get(fullClassName);
    }

    public static class RenamingConversion implements DataObjectInputStream.ObjectConverter {

        private final String newClassName;
        private Class newClass = null;

        private RenamingConversion(String newClassName) {
            this.newClassName = newClassName;
        }

        public Object readExternalizedObject(ObjectInput in) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
            Object obj = getNewClass().newInstance();
            ((Externalizable)obj).readExternal(in);
            return obj;
        }

        @Override
        public ObjectStreamClass convertClassDescriptor(ObjectStreamClass descriptor) throws ClassNotFoundException {
            return descriptor;
        }

        @Override
        public ObjectStreamClass convertResolveClassDescriptor(ObjectStreamClass descriptor) throws ClassNotFoundException {
            Class newClass = getNewClass();
            return Require.neqNull(ObjectStreamClass.lookupAny(newClass), "ObjectStreamClass.lookupAny(" + newClass + "))");
        }

        public synchronized Class getNewClass() throws ClassNotFoundException {
            if (newClass == null) {
                newClass = Class.forName(newClassName);
            }
            return newClass;
        }

        @Override
        public String toString() {
            return "RenamingConversion{newClassName='" + newClassName + "'}";
        }

    }

    public static void addRenamingConversion(String oldClass, String newClass) {
        addConversion(oldClass, new RenamingConversion(newClass));
    }

    public static void  addConversion(String oldClass, DataObjectInputStream.ObjectConverter converter) {
        converters.put(oldClass, converter);
    }
}
