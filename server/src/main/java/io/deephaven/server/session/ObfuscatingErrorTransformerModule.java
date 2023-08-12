package io.deephaven.server.session;

import dagger.Binds;
import dagger.Module;

@Module
public interface ObfuscatingErrorTransformerModule {
    @Binds
    SessionService.ErrorTransformer bindObfuscatingErrorTransformer(
            final SessionService.ObfuscatingErrorTransformer transformer);
}
