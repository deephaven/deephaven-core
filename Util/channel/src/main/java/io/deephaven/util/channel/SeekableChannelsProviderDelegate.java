//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;
import java.util.stream.Stream;

public class SeekableChannelsProviderDelegate implements SeekableChannelsProvider {
    protected final SeekableChannelsProvider delegate;

    public SeekableChannelsProviderDelegate(final SeekableChannelsProvider delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public SeekableChannelContext makeReadContext() {
        return delegate.makeReadContext();
    }

    @Override
    public WriteContext makeWriteContext() {
        return delegate.makeWriteContext();
    }

    @Override
    public boolean isCompatibleWith(@NotNull SeekableChannelContext channelContext) {
        return delegate.isCompatibleWith(channelContext);
    }

    @Override
    public boolean exists(@NotNull URI uri) {
        return delegate.exists(uri);
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext, @NotNull URI uri)
            throws IOException {
        return delegate.getReadChannel(channelContext, uri);
    }

    @Override
    public InputStream getInputStream(SeekableByteChannel channel, int sizeHint) throws IOException {
        return delegate.getInputStream(channel, sizeHint);
    }

    @Override
    public CompletableOutputStream getOutputStream(
            @NotNull final WriteContext channelContext,
            @NotNull URI uri,
            int bufferSizeHint) throws IOException {
        return delegate.getOutputStream(channelContext, uri, bufferSizeHint);
    }

    @Override
    public Stream<URI> list(@NotNull URI directory) throws IOException {
        return delegate.list(directory);
    }

    @Override
    public Stream<URI> walk(@NotNull URI directory) throws IOException {
        return delegate.walk(directory);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public SeekableChannelContext makeSingleUseReadContext() {
        return delegate.makeSingleUseReadContext();
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext, @NotNull String uriStr)
            throws IOException {
        return delegate.getReadChannel(channelContext, uriStr);
    }
}
