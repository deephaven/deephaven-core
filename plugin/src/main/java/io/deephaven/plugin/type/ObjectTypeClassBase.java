package io.deephaven.plugin.type;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

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
    public final boolean isType(Object o) {
        return clazz.isInstance(o);
    }

    @Override
    public final void writeToTypeChecked(Exporter exporter, Object object, OutputStream out) throws IOException {
        writeToImpl(exporter, clazz.cast(object), out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ObjectTypeClassBase<?> that = (ObjectTypeClassBase<?>) o;
        if (!name.equals(that.name))
            return false;
        return clazz.equals(that.clazz);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + clazz.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return name + ":" + clazz.getName();
    }
}
