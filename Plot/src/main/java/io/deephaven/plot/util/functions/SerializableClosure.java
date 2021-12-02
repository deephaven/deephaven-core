/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.util.functions;

import io.deephaven.base.classloaders.MapBackedClassLoader;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.util.GroovyDeephavenSession;
import groovy.lang.Closure;

import java.io.*;

/**
 * A serializable closure.
 */
public class SerializableClosure<T> implements Serializable {

    private static final long serialVersionUID = -1438975554979636320L;
    private Closure<T> closure;

    /**
     * Creates a SerializableClosure instance with the {@code closure}.
     *
     * @param closure closure
     */
    public SerializableClosure(final Closure<T> closure) {
        Require.neqNull(closure, "closure");
        this.closure = closure.dehydrate();
        this.closure.setResolveStrategy(Closure.DELEGATE_ONLY);
    }

    /**
     * Gets this SerializableClosure's closure.
     *
     * @return this SerializableClosure's closure
     */
    public Closure<T> getClosure() {
        return closure;
    }

    private void writeObject(final ObjectOutputStream oos) throws IOException {
        final Class c = closure.getClass();
        final String closureClassName = c.getName();

        final byte[] closureCode = GroovyDeephavenSession.getDynamicClass(closureClassName);

        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        final ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
        objectStream.writeObject(closure);
        final byte[] closureData = byteStream.toByteArray();

        oos.writeObject(closureClassName);
        oos.writeObject(closureCode);
        oos.writeObject(closureData);
    }

    @SuppressWarnings("unchecked")
    private void readObject(final ObjectInputStream ois) throws IOException, ClassNotFoundException {
        final String closureClassName = (String) ois.readObject();
        final byte[] closureCode = (byte[]) ois.readObject();
        final byte[] closureData = (byte[]) ois.readObject();

        final MapBackedClassLoader cl = new MapBackedClassLoader();
        cl.addClassData(closureClassName, closureCode);
        final ObjectInputStream objectStream =
                new CustomClassLoaderObjectInputStream<>(new ByteArrayInputStream(closureData), cl);
        closure = (Closure) objectStream.readObject();
    }

    private static class CustomClassLoaderObjectInputStream<CLT extends ClassLoader> extends ObjectInputStream {

        private final CLT classLoader;

        public CustomClassLoaderObjectInputStream(InputStream inputStream, CLT classLoader) throws IOException {
            super(inputStream);
            this.classLoader = classLoader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException, IOException {
            if (classLoader != null) {
                try {
                    return Class.forName(desc.getName(), false, classLoader);
                } catch (ClassNotFoundException cnfe) {
                    /*
                     * The default implementation in ObjectInputStream handles primitive types with a map from name to
                     * class. Rather than duplicate the functionality, we are delegating to the super method for all
                     * failures that may be of this kind, as well as any case where the passed in ClassLoader fails to
                     * find the class.
                     */
                }
            }
            return super.resolveClass(desc);
        }
    }
}
