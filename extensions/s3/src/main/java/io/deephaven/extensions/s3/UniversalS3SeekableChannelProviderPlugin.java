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

import java.util.Set;

/**
 * {@link SeekableChannelsProviderPlugin} implementation used for reading from and writing to URIs with scheme
 * {@value S3Constants#S3_URI_SCHEME}, {@value S3Constants#S3A_URI_SCHEME}, or {@value S3Constants#S3N_URI_SCHEME}.
 */
@AutoService(SeekableChannelsProviderPlugin.class)
public final class UniversalS3SeekableChannelProviderPlugin extends SeekableChannelsProviderPluginBase {

    @Override
    public boolean isCompatible(@NotNull final String uriScheme, @Nullable final Object config) {
        return isCompatible(uriScheme);
    }

    private static boolean isCompatible(@NotNull final String uriScheme) {
        return S3Constants.S3_SCHEMES.contains(uriScheme);
    }

    /**
     * Internal API for creating a {@link SeekableChannelsProvider} for S3 URIs using the provided async client.
     *
     * @param uriScheme The URI scheme to create the provider for.
     * @param s3Instructions The S3 instructions to use for the provider.
     * @param s3AsyncClient The S3 async client to use for the provider.
     */
    @InternalUseOnly
    public static SeekableChannelsProvider createUniversalS3Provider(
            @NotNull final String uriScheme,
            @NotNull final S3Instructions s3Instructions,
            @NotNull final S3AsyncClient s3AsyncClient) {
        if (!isCompatible(uriScheme)) {
            throw new IllegalArgumentException("Arguments not compatible, provided uri scheme " + uriScheme +
                    " is not compatible, expected one of " + S3Constants.S3_SCHEMES);
        }
        final S3SeekableChannelProvider impl = new S3SeekableChannelProvider(s3Instructions, s3AsyncClient);
        return createdProviderImplHelper(uriScheme, impl);
    }

    @Override
    protected SeekableChannelsProvider createProviderImpl(
            @NotNull final String uriScheme,
            @Nullable final Object config) {
        return createdProviderImplHelper(uriScheme, create(config));
    }

    private static SeekableChannelsProvider createdProviderImplHelper(
            @NotNull final String uriScheme,
            @NotNull final S3SeekableChannelProvider impl) {
        switch (uriScheme) {
            case S3Constants.S3_URI_SCHEME:
                return impl;
            case S3Constants.S3A_URI_SCHEME:
                return new S3DelegateProvider(S3Constants.S3A_URI_SCHEME, impl);
            case S3Constants.S3N_URI_SCHEME:
                return new S3DelegateProvider(S3Constants.S3N_URI_SCHEME, impl);
            default:
                throw new IllegalStateException("Unexpected uriScheme: " + uriScheme);
        }
    }

    @Override
    protected SeekableChannelsProvider createProviderImpl(@NotNull Set<String> uriSchemes,
            @Nullable final Object config) {
        final S3SeekableChannelProvider impl = create(config);
        final S3SeekableChannelProvider s3 = uriSchemes.contains(S3Constants.S3_URI_SCHEME)
                ? impl
                : null;
        final S3DelegateProvider s3a = uriSchemes.contains(S3Constants.S3A_URI_SCHEME)
                ? new S3DelegateProvider(S3Constants.S3A_URI_SCHEME, impl)
                : null;
        final S3DelegateProvider s3n = uriSchemes.contains(S3Constants.S3N_URI_SCHEME)
                ? new S3DelegateProvider(S3Constants.S3N_URI_SCHEME, impl)
                : null;
        return new UniversalS3Provider(impl, s3, s3a, s3n);
    }

    private S3SeekableChannelProvider create(@Nullable final Object config) {
        if (config != null && !(config instanceof S3Instructions)) {
            throw new IllegalArgumentException("Only S3Instructions are valid when reading files from S3, provided " +
                    "config instance of class " + config.getClass().getName());
        }
        return new S3SeekableChannelProvider(config == null ? S3Instructions.DEFAULT : (S3Instructions) config);
    }
}
