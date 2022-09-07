package io.deephaven.server.runner;

import dagger.Module;
import dagger.Provides;
import io.deephaven.engine.context.ExecutionContext;

import javax.inject.Singleton;

@Module
public class ExecutionContextUnitTestModule {
    @Provides
    @Singleton
    public ExecutionContext provideExecutionContext() {
        return ExecutionContext.createForUnitTests();
    }
}
