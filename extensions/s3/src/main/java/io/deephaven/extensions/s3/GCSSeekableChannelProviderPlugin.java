//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import com.google.auto.service.AutoService;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderPlugin;
import io.deephaven.util.channel.SeekableChannelsProviderPluginBase;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.net.URI;

/**
 * {@link SeekableChannelsProviderPlugin} implementation used for reading files from Google Cloud Storage.
 */
@AutoService(SeekableChannelsProviderPlugin.class)
public final class GCSSeekableChannelProviderPlugin extends SeekableChannelsProviderPluginBase {

    static final String GCS_URI_SCHEME = "gs";

    private static final String ENDPOINT_OVERRIDE_SUFFIX = ".googleapis.com";
    private static final URI DEFAULT_ENDPOINT_OVERRIDE = URI.create("https://storage.googleapis.com");
    private static final S3Instructions DEFAULT_INSTRUCTIONS =
            S3Instructions.builder().endpointOverride(DEFAULT_ENDPOINT_OVERRIDE).build();

    @Override
    public boolean isCompatible(@NotNull final String uriScheme, @Nullable final Object config) {
        return GCS_URI_SCHEME.equals(uriScheme);
    }

    /**
     * Internal API for creating a {@link SeekableChannelsProvider} for reading from and writing to GCS URIs
     *
     * @param config The configuration object for the provider.
     * @param s3AsyncClient The S3 async client to use for the provider.
     */
    static SeekableChannelsProvider createGCSSeekableChannelProvider(
            @Nullable final Object config,
            @NotNull final S3AsyncClient s3AsyncClient) {
        final S3SeekableChannelProvider impl =
                new S3SeekableChannelProvider(normalizeS3Instructions(config), s3AsyncClient);
        return new S3DelegateProvider(GCS_URI_SCHEME, impl);
    }

    /**
     * Internal API for creating a {@link SeekableChannelsProvider} for reading from and writing to GCS URIs.
     *
     * @param config The configuration object for the provider.
     */
    static SeekableChannelsProvider createGCSSeekableChannelProvider(
            @Nullable final Object config) {
        final S3SeekableChannelProvider impl = new S3SeekableChannelProvider(normalizeS3Instructions(config));
        return new S3DelegateProvider(GCS_URI_SCHEME, impl);
    }

    @Override
    protected SeekableChannelsProvider createProviderImpl(@NotNull final String uriScheme,
            @Nullable final Object config) {
        return createGCSSeekableChannelProvider(config);
    }

    /**
     * Normalize the provided config to ensure valid instructions for reading from GCS.
     */
    private static S3Instructions normalizeS3Instructions(@Nullable final Object config) {
        if (config == null) {
            return DEFAULT_INSTRUCTIONS;
        }
        if (!(config instanceof S3Instructions)) {
            throw new IllegalArgumentException("Only S3Instructions are valid when reading GCS URIs, " +
                    "provided config instance of class " + config.getClass().getName());
        }
        final S3Instructions s3Instructions = (S3Instructions) config;
        if (s3Instructions.endpointOverride().isEmpty()) {
            return s3Instructions.withEndpointOverride(DEFAULT_ENDPOINT_OVERRIDE);
        }
        if (!(s3Instructions.endpointOverride().get()).toString().endsWith(ENDPOINT_OVERRIDE_SUFFIX)) {
            throw new IllegalArgumentException("Provided endpoint override=(" +
                    s3Instructions.endpointOverride().get() + " not supported when reading GCS URIs, must end with " +
                    ENDPOINT_OVERRIDE_SUFFIX);
        }
        return s3Instructions;
    }
}

