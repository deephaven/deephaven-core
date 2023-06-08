package io.deephaven.client;

import dagger.Module;
import dagger.Provides;
import io.deephaven.client.impl.ClientChannelFactory;

/**
 * Provides {@link ClientChannelFactory}.
 */
@Module
public interface ClientDefaultsModule {


    /**
     * Equivalent to {@link ClientChannelFactory#defaultInstance()}.
     */
    @Provides
    static ClientChannelFactory providesClientChannelFactory() {
        return ClientChannelFactory.defaultInstance();
    }
}
