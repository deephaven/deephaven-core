package io.deephaven.util.channel;

import io.deephaven.util.SafeCloseable;

import java.io.Closeable;
import java.util.function.Supplier;

/**
 * Context object for reading and writing to channels created by {@link SeekableChannelsProvider}.
 */
public interface SeekableChannelContext extends SafeCloseable {

    SeekableChannelContext NULL = SeekableChannelContextNull.NULL;

    static Provider upgrade(SeekableChannelsProvider provider, SeekableChannelContext context) {
        if (context != NULL) {
            return () -> context;
        }
        return new ProviderImpl(provider.makeSingleUseContext());
    }

    /**
     * Release any resources associated with this context. The context should not be used afterward.
     */
    default void close() {}

    interface Provider extends Closeable, Supplier<SeekableChannelContext> {

        @Override
        SeekableChannelContext get();

        @Override
        default void close() {}
    }
}
