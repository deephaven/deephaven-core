//
// Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console.python;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.deephaven.util.thread.ThreadInitializationFactory;

@Module
public interface PythonDebuggingModule {
    @Binds
    @IntoSet
    ThreadInitializationFactory bindDebuggingInitializer(DebuggingInitializer debuggingInitializer);
}
