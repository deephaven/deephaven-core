//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit S3ASeekableChannelProviderPlugin and run "./gradlew replicateChannelProviders" to regenerate
//
// @formatter:off
package io.deephaven.extensions.s3;

import com.google.auto.service.AutoService;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderPlugin;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link SeekableChannelsProviderPlugin} implementation used for reading from and writing to URIs with schema "s3n".
 */
@AutoService(SeekableChannelsProviderPlugin.class)
public final class S3NSeekableChannelProviderPlugin implements SeekableChannelsProviderPlugin {

    static final String S3N_URI_SCHEME = "s3n";

    @Override
    public boolean isCompatible(@NotNull final String uriScheme, @Nullable final Object config) {
        return S3N_URI_SCHEME.equals(uriScheme);
    }

    @Override
    public SeekableChannelsProvider createProvider(@NotNull final String uriScheme, @Nullable final Object config) {
        if (!isCompatible(uriScheme, config)) {
            throw new IllegalArgumentException("Arguments not compatible, provided uri scheme " + uriScheme);
        }
        if (config != null && !(config instanceof S3Instructions)) {
            throw new IllegalArgumentException("Only S3Instructions are valid when reading files from S3, provided " +
                    "config instance of class " + config.getClass().getName());
        }
        return new S3NSeekableChannelProvider(config == null ? S3Instructions.DEFAULT : (S3Instructions) config);
    }
}
