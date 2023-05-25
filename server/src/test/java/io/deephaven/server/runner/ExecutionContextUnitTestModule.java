package io.deephaven.server.runner;

import dagger.Module;
import dagger.Provides;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;

import javax.inject.Singleton;

@Module
public class ExecutionContextUnitTestModule {
    @Provides
    @Singleton
    public ExecutionContext provideExecutionContext() {
        return TestExecutionContext.createForUnitTests();
    }
}
