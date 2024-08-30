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
import java.net.URISyntaxException;
import java.util.function.Function;

import static io.deephaven.extensions.s3.S3SeekableChannelProviderPlugin.S3_URI_SCHEME;

/**
 * {@link SeekableChannelsProviderPlugin} implementation used for reading files from S3.
 */
@AutoService(SeekableChannelsProviderPlugin.class)
public final class GCSSeekableChannelProviderPlugin implements SeekableChannelsProviderPlugin {

    private static final String GCS_URI_SCHEME = "gs";

    private static final Function<URI, URI> GCS_TO_S3_URI_CONVERTER = uri -> {
        try {
            if (S3_URI_SCHEME.equals(uri.getScheme())) {
                return uri;
            }
            return new URI(S3_URI_SCHEME, uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(),
                    uri.getQuery(), uri.getFragment());
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Failed to convert GCS URI " + uri + " to s3 URI", e);
        }
    };

    @Override
    public boolean isCompatible(@NotNull final URI uri, @Nullable final Object config) {
        return GCS_URI_SCHEME.equals(uri.getScheme());
    }

    @Override
    public SeekableChannelsProvider createProvider(@NotNull final URI uri, @Nullable final Object config) {
        if (!isCompatible(uri, config)) {
            throw new IllegalArgumentException("Arguments not compatible, provided uri " + uri);
        }
        return new S3SeekableChannelProvider(s3Instructions(config), GCS_TO_S3_URI_CONVERTER);
    }

    /**
     * Get the S3Instructions from the config object, or use the default if the config is null.
     */
    private static S3Instructions s3Instructions(@Nullable final Object config) {
        if (config == null) {
            return S3Instructions.DEFAULT_FOR_GCS_URI;
        }
        if (!(config instanceof S3Instructions)) {
            throw new IllegalArgumentException("Only S3Instructions are valid when reading GCS URIs, " +
                    "provided config instance of class " + config.getClass().getName());
        }
        final S3Instructions s3Instructions = (S3Instructions) config;
        if (s3Instructions.endpointOverride().isEmpty()) {
            return s3Instructions.withEndpointOverride(S3Instructions.DEFAULT_ENDPOINT_OVERRIDE_FOR_GCS);
        }
        if (!S3Instructions.DEFAULT_ENDPOINT_OVERRIDE_FOR_GCS.equals(s3Instructions.endpointOverride().get())) {
            // TODO Check with Devin what to do in this case, because "http" instead of "https" might also work.
            throw new IllegalArgumentException("Provided endpoint override=(" +
                    s3Instructions.endpointOverride().get() + " not supported when reading GCS URIs");
        }
        return s3Instructions;
    }
}

