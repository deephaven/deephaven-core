package io.deephaven.web.shared.cmd;

import java.io.Serializable;

/**
 * Like a TableHandle, but not constrained to tables (i.e. not always a TableHandle).
 *
 * This allows us to build infrastructure like metrics without requiring use of table handle semantics.
 */
public abstract class ServerReplyHandle implements Serializable {

    /**
     * Use this in public no-arg constructors for serialization; serialization is allowed to overwrite final fields, so
     * just call this(DESERIALIZATION_IN_PROGRESS); inside your no-arg ctors.
     */
    public static final int DESERIALIZATION_IN_PROGRESS = -3;

    public static final int UNINITIALIZED = -1;

    public abstract int getClientId();

}
