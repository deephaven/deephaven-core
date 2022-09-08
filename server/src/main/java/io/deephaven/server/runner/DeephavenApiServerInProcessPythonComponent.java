/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.runner;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.server.console.SessionToExecutionStateModule;
import io.deephaven.server.console.python.PythonConsoleSessionModule;
import io.deephaven.server.console.python.PythonGlobalScopeCopyModule;
import io.deephaven.server.log.LogModule;
import io.grpc.ManagedChannelBuilder;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.PrintStream;

@Singleton
@Component(modules = {
        DeephavenApiServerModule.class,
        LogModule.class,
        PythonConsoleSessionModule.class,
        PythonGlobalScopeCopyModule.class,
        ServerBuilderInProcessModule.class,
        SessionToExecutionStateModule.class,
})
public interface DeephavenApiServerInProcessPythonComponent {

    DeephavenApiServer getServer();

    ManagedChannelBuilder<?> channelBuilder();

    @Component.Builder
    interface Builder {

        @BindsInstance
        Builder withSchedulerPoolSize(@Named("scheduler.poolSize") int numThreads);

        @BindsInstance
        Builder withSessionTokenExpireTmMs(@Named("session.tokenExpireMs") long tokenExpireMs);

        @BindsInstance
        Builder withOut(@Named("out") PrintStream out);

        @BindsInstance
        Builder withErr(@Named("err") PrintStream err);

        DeephavenApiServerInProcessPythonComponent build();
    }
}
