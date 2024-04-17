//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.Nullable;

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
     * Minimal interface for optional, opaque objects hosted by channel context instances.
     */
    interface Resource {
    }

    /**
     * Set the opaque {@link Resource} object hosted by this instance.
     */
    void setResource(Resource resource);

    /**
     * @return An opaque {@link Resource} object hosted by this instance, or {@code null} if none has been set
     */
    @Nullable
    <RESOURCE_TYPE extends Resource> RESOURCE_TYPE getResource();

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
