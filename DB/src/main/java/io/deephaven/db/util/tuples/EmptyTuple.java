package io.deephaven.db.util.tuples;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.function.UnaryOperator;

/**
 * <p>
 * 0-Tuple key class.
 */
public enum EmptyTuple
    implements Comparable<EmptyTuple>, Serializable, CanonicalizableTuple<EmptyTuple> {

    INSTANCE;

    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return "EmptyTuple";
    }

    @Override
    public EmptyTuple canonicalize(@NotNull final UnaryOperator<Object> canonicalizer) {
        return this;
    }
}
