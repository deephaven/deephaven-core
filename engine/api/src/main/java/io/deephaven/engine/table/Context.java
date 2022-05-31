package io.deephaven.engine.table;

import io.deephaven.util.SafeCloseable;

/**
 * Base interface for state/mutable data that needs to be kept over the course of an evaluation session for a Chunk
 * Source, Functor or Sink.
 */
public interface Context extends SafeCloseable {
    /**
     * Release any resources associated with this context. The context should not be used afterwards.
     */
    default void close() {}

}
