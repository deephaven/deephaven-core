//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import com.google.auto.service.AutoService;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderPlugin;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;

/**
 * {@link SeekableChannelsProviderPlugin} implementation used for reading files from S3.
 */
@AutoService(SeekableChannelsProviderPlugin.class)
public final class S3SeekableChannelProviderPlugin implements SeekableChannelsProviderPlugin {

    private static final String S3_URI_SCHEME = "s3";

    @Override
    public boolean isCompatible(@NotNull final URI uri, @Nullable final Object config) {
        return S3_URI_SCHEME.equals(uri.getScheme());
    }

    @Override
    public SeekableChannelsProvider createProvider(@NotNull final URI uri, @Nullable final Object config) {
        if (!isCompatible(uri, config)) {
            if (!(config instanceof S3Instructions)) {
                throw new IllegalArgumentException("Must provide S3Instructions to read files from S3");
            }
            throw new IllegalArgumentException("Arguments not compatible, provided uri " + uri);
        }
        final S3Instructions s3Instructions = (S3Instructions) config;
        return new S3SeekableChannelProvider(s3Instructions);
    }
}
