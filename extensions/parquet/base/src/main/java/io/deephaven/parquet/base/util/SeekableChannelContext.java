package io.deephaven.parquet.base.util;

import io.deephaven.util.SafeCloseable;

/**
 * Context object for reading and writing to channels created by {@link SeekableChannelsProvider}.
 */
public interface SeekableChannelContext extends SafeCloseable {

    SeekableChannelContext NULL = new SeekableChannelContext() {};

    /**
     * Release any resources associated with this context. The context should not be used afterward.
     */
    default void close() {}
}
