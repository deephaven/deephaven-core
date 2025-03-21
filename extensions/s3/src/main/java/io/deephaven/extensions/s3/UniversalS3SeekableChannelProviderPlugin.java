//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import com.google.auto.service.AutoService;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderPlugin;
import io.deephaven.util.channel.SeekableChannelsProviderPluginBase;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

/**
 * {@link SeekableChannelsProviderPlugin} implementation used for reading from and writing to URIs with schema "s3a".
 */
@AutoService(SeekableChannelsProviderPlugin.class)
public final class UniversalS3SeekableChannelProviderPlugin extends SeekableChannelsProviderPluginBase {

    static final String S3N_URI_SCHEME = "s3n";
    static final String S3A_URI_SCHEME = "s3a";
    static final String S3_URI_SCHEME = "s3";

    @Override
    public boolean isCompatible(@NotNull final String uriScheme, @Nullable final Object config) {
        return S3_URI_SCHEME.equals(uriScheme) || S3A_URI_SCHEME.equals(uriScheme) || S3N_URI_SCHEME.equals(uriScheme);
    }

    @Override
    protected SeekableChannelsProvider createProviderImpl(@NotNull final String uriScheme,
            @Nullable final Object config) {
        final S3SeekableChannelProvider impl = create(config);
        switch (uriScheme) {
            case S3_URI_SCHEME:
                return impl;
            case S3A_URI_SCHEME:
                return new S3DelegateProvider(S3A_URI_SCHEME, impl);
            case S3N_URI_SCHEME:
                return new S3DelegateProvider(S3N_URI_SCHEME, impl);
            default:
                throw new IllegalStateException("Unexpected uriScheme: " + uriScheme);
        }
    }

    @Override
    protected SeekableChannelsProvider createProviderImpl(@NotNull Set<String> uriSchemes,
            @Nullable final Object config) {
        final S3SeekableChannelProvider impl = create(config);
        final S3SeekableChannelProvider s3 = uriSchemes.contains(S3_URI_SCHEME)
                ? impl
                : null;
        final S3DelegateProvider s3a = uriSchemes.contains(S3A_URI_SCHEME)
                ? new S3DelegateProvider(S3A_URI_SCHEME, impl)
                : null;
        final S3DelegateProvider s3n = uriSchemes.contains(S3N_URI_SCHEME)
                ? new S3DelegateProvider(S3N_URI_SCHEME, impl)
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
