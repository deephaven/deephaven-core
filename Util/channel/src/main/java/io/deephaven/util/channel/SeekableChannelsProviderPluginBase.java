//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.stream.Collectors;

public abstract class SeekableChannelsProviderPluginBase implements SeekableChannelsProviderPlugin {

    @Override
    public final boolean isCompatible(@NotNull Set<String> uriSchemes, @Nullable Object config) {
        if (uriSchemes.isEmpty()) {
            throw new IllegalArgumentException("uriSchemes must be non-empty");
        }
        for (String uriScheme : uriSchemes) {
            if (!isCompatible(uriScheme, config)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public final SeekableChannelsProvider createProvider(@NotNull String uriScheme, @Nullable Object object) {
        if (!isCompatible(uriScheme, object)) {
            throw new IllegalArgumentException(String.format("Not compatible with uri scheme `%s`", uriScheme));
        }
        return createProviderImpl(uriScheme, object);
    }

    @Override
    public final SeekableChannelsProvider createProvider(@NotNull Set<String> uriSchemes, @Nullable Object object) {
        if (!isCompatible(uriSchemes, object)) {
            throw new IllegalArgumentException(String.format("Not compatible with uri schemes [%s]",
                    uriSchemes.stream().collect(Collectors.joining("`, `", "`", "`"))));
        }
        if (uriSchemes.size() == 1) {
            return createProviderImpl(uriSchemes.iterator().next(), object);
        }
        return createProviderImpl(uriSchemes, object);
    }

    protected abstract SeekableChannelsProvider createProviderImpl(@NotNull String uriScheme, @Nullable Object object);

    protected SeekableChannelsProvider createProviderImpl(@NotNull Set<String> uriSchemes, @Nullable Object object) {
        throw new UnsupportedOperationException();
    }
}
