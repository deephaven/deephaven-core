package io.deephaven.plugin.type;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * An implementation that uses strict {@link Class} equality for the {@link #isType(Object)} check.
 * 
 * @param <T> the class type
 */
public abstract class ObjectTypeClassBase<T> extends ObjectTypeBase {
    private final String name;
    private final Class<T> clazz;

    public ObjectTypeClassBase(String name, Class<T> clazz) {
        this.name = Objects.requireNonNull(name);
        this.clazz = Objects.requireNonNull(clazz);
    }

    public abstract void writeToImpl(Exporter exporter, T object, OutputStream out) throws IOException;

    public final Class<T> clazz() {
        return clazz;
    }

    @Override
    public final String name() {
        return name;
    }

    @Override
    public final boolean isType(Object object) {
        return clazz.equals(object.getClass());
    }

    @Override
    public final void writeCompatibleObjectTo(Exporter exporter, Object object, OutputStream out) throws IOException {
        // noinspection unchecked
        writeToImpl(exporter, (T) object, out);
    }

    @Override
    public String toString() {
        return name + ":" + clazz.getName();
    }
}
