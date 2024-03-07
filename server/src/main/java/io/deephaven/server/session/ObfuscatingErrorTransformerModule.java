//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import dagger.Binds;
import dagger.Module;

@Module
public interface ObfuscatingErrorTransformerModule {
    @Binds
    SessionService.ErrorTransformer bindObfuscatingErrorTransformer(
            final SessionService.ObfuscatingErrorTransformer transformer);
}
