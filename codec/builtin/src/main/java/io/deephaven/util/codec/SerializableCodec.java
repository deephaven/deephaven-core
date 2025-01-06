//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.codec;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;

public class SerializableCodec<T> implements ObjectCodec<T> {

    private static final SerializableCodec INSTANCE = new SerializableCodec();

    public static <T> SerializableCodec<T> create() {
        // noinspection unchecked
        return INSTANCE;
    }

    private SerializableCodec() {}

    public SerializableCodec(@SuppressWarnings("unused") String dummy) {}

    @Override
    public byte @NotNull [] encode(@Nullable T input) {
        try {
            final ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
            final ObjectOutputStream objectOutput = new ObjectOutputStream(byteOutput);
            objectOutput.writeObject(input);
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
            // noinspection unchecked
            return (T) objectInput.readObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int expectedObjectWidth() {
        return VARIABLE_WIDTH_SENTINEL;
    }
}
