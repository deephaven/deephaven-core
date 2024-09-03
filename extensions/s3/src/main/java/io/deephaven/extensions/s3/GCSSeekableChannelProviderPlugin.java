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

import static io.deephaven.extensions.s3.S3Instructions.DEFAULT_ENDPOINT_OVERRIDE_FOR_GCS;

/**
 * {@link SeekableChannelsProviderPlugin} implementation used for reading files from Google Cloud Storage.
 */
@AutoService(SeekableChannelsProviderPlugin.class)
public final class GCSSeekableChannelProviderPlugin implements SeekableChannelsProviderPlugin {

    static final String GCS_URI_SCHEME = "gs";
    private static final String ENDPOINT_OVERRIDE_SUFFIX = ".googleapis.com";

    @Override
    public boolean isCompatible(@NotNull final URI uri, @Nullable final Object config) {
        return GCS_URI_SCHEME.equals(uri.getScheme());
    }

    @Override
    public SeekableChannelsProvider createProvider(@NotNull final URI uri, @Nullable final Object config) {
        if (!isCompatible(uri, config)) {
            throw new IllegalArgumentException("Arguments not compatible, provided uri " + uri);
        }
        return new GCSSeekableChannelProvider(s3Instructions(config));
    }

    /**
     * Get the S3Instructions from the config object, or use the default if the config is null.
     */
    private static S3Instructions s3Instructions(@Nullable final Object config) {
        if (config == null) {
            return S3Instructions.DEFAULT_FOR_GCS;
        }
        if (!(config instanceof S3Instructions)) {
            throw new IllegalArgumentException("Only S3Instructions are valid when reading GCS URIs, " +
                    "provided config instance of class " + config.getClass().getName());
        }
        final S3Instructions s3Instructions = (S3Instructions) config;
        if (s3Instructions.endpointOverride().isEmpty()) {
            return s3Instructions.withEndpointOverride(DEFAULT_ENDPOINT_OVERRIDE_FOR_GCS);
        }
        if (!(s3Instructions.endpointOverride().get()).toString().endsWith(ENDPOINT_OVERRIDE_SUFFIX)) {
            throw new IllegalArgumentException("Provided endpoint override=(" +
                    s3Instructions.endpointOverride().get() + " not supported when reading GCS URIs, must end with " +
                    ENDPOINT_OVERRIDE_SUFFIX);
        }
        return s3Instructions;
    }
}

