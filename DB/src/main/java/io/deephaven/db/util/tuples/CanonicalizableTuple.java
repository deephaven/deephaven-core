package io.deephaven.db.util.tuples;

import org.jetbrains.annotations.NotNull;

import java.util.function.UnaryOperator;

/**
 * Interface for immutable tuple classes that can produce a new instance of themselves with canonicalized object
 * elements.
 */
public interface CanonicalizableTuple<TUPLE_TYPE> {

    /**
     * Canonicalize this tuple.
     *
     * @param canonicalizer The canonicalization operator to use on each object element.
     * @return This tuple if already canonical, else a new, canonical tuple of the same type
     */
    TUPLE_TYPE canonicalize(@NotNull UnaryOperator<Object> canonicalizer);
}
