//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.codec;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.lang.reflect.InvocationTargetException;

public class ExternalizableCodec<T extends Externalizable> implements ObjectCodec<T> {

    private final Class<T> externalizableClass;

    public ExternalizableCodec(String className) {
        try {
            // noinspection unchecked
            this.externalizableClass = (Class<T>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte @NotNull [] encode(@Nullable T input) {
        if (input == null) {
            throw new UnsupportedOperationException(getClass() + " does not support null input");
        }
        try {
            final ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
            final ObjectOutputStream objectOutput = new ObjectOutputStream(byteOutput);
            input.writeExternal(objectOutput);
            objectOutput.flush();
            return byteOutput.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Nullable
    @Override
    public T decode(byte @NotNull [] input, int offset, int length) {
        try {
            final ByteArrayInputStream byteInput = new ByteArrayInputStream(input, offset, length);
            final ObjectInputStream objectInput = new ObjectInputStream(byteInput);
            T result = externalizableClass.getDeclaredConstructor().newInstance();
            result.readExternal(objectInput);
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException | InvocationTargetException
                | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int expectedObjectWidth() {
        return VARIABLE_WIDTH_SENTINEL;
    }
}
