//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import java.net.URI;

public interface SeekableChannelsProviderFactory {

    /**
     * Create a {@link SeekableChannelsProvider} compatible for reading and writing to the given URI.
     *
     * @param uri The URI
     * @param specialInstructions An optional object to pass special instructions to the provider.
     */
    SeekableChannelsProvider createProvider(URI uri, Object specialInstructions);
}
