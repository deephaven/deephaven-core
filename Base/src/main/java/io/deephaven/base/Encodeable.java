/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This interface is a slightly weaker version of java.io.Externalizable, in that it only allows the use of the
 * DataInput and -Output interfaces for reading and writing, not ObjectInput and -Output.
 */
public interface Encodeable {
    public void encode(DataOutput out) throws IOException;

    public void decode(DataInput in) throws IOException;
}
