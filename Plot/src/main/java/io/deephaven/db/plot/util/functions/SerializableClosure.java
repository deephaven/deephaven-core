/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.util.functions;

import io.deephaven.base.classloaders.MapBackedClassLoader;
import io.deephaven.base.verify.Require;
import io.deephaven.dataobjects.persistence.CustomClassLoaderObjectInputStream;
import io.deephaven.db.util.GroovyDeephavenSession;
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
        final ObjectInputStream objectStream = new CustomClassLoaderObjectInputStream<>(new ByteArrayInputStream(closureData), cl);
        closure = (Closure) objectStream.readObject();
    }
}
