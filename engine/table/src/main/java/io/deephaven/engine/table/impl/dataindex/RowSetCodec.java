//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.ExternalizableRowSetUtils;
import io.deephaven.util.codec.ObjectCodec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;

/**
 * A codec to encode and decode generic row set to a column.
 */
public class RowSetCodec implements ObjectCodec<RowSet> {

    public RowSetCodec(@SuppressWarnings("unused") String arguments) {}

    @Override
    @NotNull
    public byte[] encode(@Nullable final RowSet input) {
        if (input == null) {
            throw new UnsupportedOperationException(getClass() + " does not support null input");
        }
        try {
            final ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
            final DataOutputStream dataOutputStream = new DataOutputStream(byteOutput);
            ExternalizableRowSetUtils.writeExternalCompressedDeltas(dataOutputStream, input);
            dataOutputStream.flush();
            return byteOutput.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isNullable() {
        return false;
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
    public RowSet decode(@NotNull final byte[] input, final int offset, final int length) {
        try {
            final ByteArrayInputStream byteInput = new ByteArrayInputStream(input, offset, length);
            final DataInputStream dataInputStream = new DataInputStream(byteInput);
            return ExternalizableRowSetUtils.readExternalCompressedDelta(dataInputStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int expectedObjectWidth() {
        return VARIABLE_WIDTH_SENTINEL;
    }
}
