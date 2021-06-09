/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;

/**
 * The type of a table, which from a user perspective generally corresponds to System vs. User namespaces, and whether
 * the table is a historical (permanent) or intraday (ticking) table.
 */
public enum TableType implements LogOutputAppendable {

    //TODO: Make TT an int - byte 0 is NS-set, byte 1 is data flavor, byte 2 is nIPs, byte 3 is nCPs.

    /**
     * Stand-alone tables opened outside of a table data service.
     * These tables are always unpartitioned.
     * (i.e. storage type is TableDefinition.STORAGETYPE_SPLAYEDONDISK).
     */
    STANDALONE_SPLAYED((byte)'X');

    private final byte wireValue;
    private final String descriptor;

    TableType(byte wireValue) {
        this.wireValue = wireValue;
        this.descriptor = new String(new char[]{(char)wireValue});
    }
    public byte getWireValue() {
        return wireValue;
    }

    public String getDescriptor() {
        return descriptor;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append(name());
    }

    /**
     * Convert the wireValue to a TableType.
     *
     * @param wireValue the wire value to lookup
     *
     * @return the TableType, or null if invalid
     */
    public static TableType lookup(byte wireValue) {
        switch (wireValue) {
            case 'X': return STANDALONE_SPLAYED;
        }
        return null;
    }
}
