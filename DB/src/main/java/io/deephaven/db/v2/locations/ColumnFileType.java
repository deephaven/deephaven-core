/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Column data file types in the Iris persistent storage format.
 */
public enum ColumnFileType implements LogOutputAppendable {

    /**
     * Primary column data.
     */
    DATA((byte)'D'),

    /**
     * Binary referenced data.
     * <b>Note:</b> This only relevant to variable-length data columns.
     * <b>Note:</b> The primary data specifies (long) offsets into this data.
     */
    BINARY_DATA((byte)'B'),

    /**
     * Symbol offset referenced data.
     * <b>Note:</b> This is only relevant to symbol columns.
     * <b>Note:</b> The primary data specifies (integer) indexes into this data.
     */
    SYMBOL_OFFSET((byte)'O'),

    /**
     * Symbol binary referenced data.
     * <b>Note:</b> This is only relevant to symbol columns.
     * <b>Note:</b> The symbol offset referenced data specifies (long) offsets into this data.
     */
    SYMBOL_BINARY_DATA((byte)'S');

    private final byte wireValue;

    ColumnFileType(byte wireValue) {
        this.wireValue = wireValue;
    }

    @SuppressWarnings("unused")
    public byte getWireValue() {
        return wireValue;
    }

    public void writeTo(DataOutput output) throws IOException {
        output.writeByte(wireValue);
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append(name());
    }

    public static ColumnFileType lookup(byte wireValue) {
        switch (wireValue) {
            case 'D': return DATA;
            case 'B': return BINARY_DATA;
            case 'O': return SYMBOL_OFFSET;
            case 'S': return SYMBOL_BINARY_DATA;
        }
        return null;
    }

    public static ColumnFileType readFrom(DataInput input) throws IOException {
        final byte wireValue = input.readByte();
        final ColumnFileType result = lookup(wireValue);
        if(result == null) {
            throw new IOException("Unknown wireValue " + wireValue + '(' + (char)wireValue + ')');
        }
        return result;
    }
}
