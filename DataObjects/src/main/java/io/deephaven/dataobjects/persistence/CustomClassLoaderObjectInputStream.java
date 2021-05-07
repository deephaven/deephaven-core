/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.dataobjects.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

public class CustomClassLoaderObjectInputStream<CLT extends ClassLoader> extends ObjectInputStream {

    private final CLT classLoader;

    public CustomClassLoaderObjectInputStream(InputStream inputStream, CLT classLoader) throws IOException {
        super(inputStream);
        this.classLoader = classLoader;
    }

    public CustomClassLoaderObjectInputStream(InputStream inputStream) throws IOException {
        this(inputStream, null);
    }

    public CLT getClassLoader() {
        return classLoader;
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException, IOException {
        DataObjectInputStream.ObjectConverter converter = DataObjectInputConversions.getObjectConverter(desc.getName());
        if (converter != null && converter instanceof DataObjectInputConversions.RenamingConversion) {
            return ((DataObjectInputConversions.RenamingConversion) converter).getNewClass();
        }

        if(classLoader != null) {
            try {
                return Class.forName(desc.getName(), false, classLoader);
            } catch (ClassNotFoundException cnfe) {
                /* The default implementation in ObjectInputStream handles primitive types with a map from name to class.
                 * Rather than duplicate the functionality, I'm delegating to the super method for all failures that
                 * may be of this kind, as well as any case where the passed in ClassLoader fails to find the class.
                 */
            }
        }
        return super.resolveClass(desc);
    }
}
