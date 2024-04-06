//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import io.deephaven.util.SafeCloseable;

import java.util.function.Supplier;

/**
 * Context object for reading and writing to channels created by {@link SeekableChannelsProvider}.
 */
public interface SeekableChannelContext extends SafeCloseable {

    SeekableChannelContext NULL = SeekableChannelContextNull.NULL_CONTEXT_INSTANCE;

    /**
     * A pattern that allows callers to ensure a valid context has been created for {@code provider}. In the case where
     * the given {@code context} {@link SeekableChannelsProvider#isCompatibleWith(SeekableChannelContext) is compatible
     * with} {@code provider}, a no-op holder around that {@code context} will be returned. Otherwise, a holder with a
     * new {@link SeekableChannelsProvider#makeSingleUseContext()} will be returned. The returned holder should ideally
     * be used in a try-with-resources construction.
     *
     * @param provider the provider
     * @param context the context
     * @return the context holder
     */
    static ContextHolder ensureContext(SeekableChannelsProvider provider, SeekableChannelContext context) {
        if (!provider.isCompatibleWith(context)) {
            return new ContextHolderImpl(provider.makeSingleUseContext());
        }
        // An impl that does not close the context
        return () -> context;
    }

    /**
     * Release any resources associated with this context. The context should not be used afterward.
     */
    default void close() {}

    interface ContextHolder extends AutoCloseable, Supplier<SeekableChannelContext> {

        @Override
        SeekableChannelContext get();

        @Override
        default void close() {}
    }
}
