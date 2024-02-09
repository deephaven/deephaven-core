/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.channel;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;

public abstract class SeekableChannelsProviderBase implements SeekableChannelsProvider {

    protected abstract boolean readChannelIsBuffered();

    @Override
    public final InputStream getInputStream(SeekableByteChannel channel) {
        final InputStream in = Channels.newInputStream(ReadableByteChannelNoClose.of(channel));
        return readChannelIsBuffered() ? in : new BufferedInputStream(in, 8192);
    }
}
