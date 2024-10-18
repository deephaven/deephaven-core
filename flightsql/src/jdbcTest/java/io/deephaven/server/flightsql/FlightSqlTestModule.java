//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.base.clock.Clock;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.AbstractScriptSession;
import io.deephaven.engine.util.NoLanguageDeephavenSession;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.config.ConfigServiceModule;
import io.deephaven.server.console.ConsoleModule;
import io.deephaven.server.log.LogModule;
import io.deephaven.server.plugin.PluginsModule;
import io.deephaven.server.session.ExportTicketResolver;
import io.deephaven.server.session.ObfuscatingErrorTransformerModule;
import io.deephaven.server.session.SessionModule;
import io.deephaven.server.session.TicketResolver;
import io.deephaven.server.table.TableModule;
import io.deephaven.server.test.TestAuthModule;
import io.deephaven.server.test.TestAuthorizationProvider;
import io.deephaven.server.util.Scheduler;
import org.jetbrains.annotations.Nullable;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Module(includes = {
        ArrowModule.class,
        ConfigServiceModule.class,
        ConsoleModule.class,
        LogModule.class,
        SessionModule.class,
        TableModule.class,
        TestAuthModule.class,
        ObfuscatingErrorTransformerModule.class,
        PluginsModule.class,
        FlightSqlModule.class
})
public class FlightSqlTestModule {
    @IntoSet
    @Provides
    TicketResolver ticketResolver(ExportTicketResolver resolver) {
        return resolver;
    }

    @Singleton
    @Provides
    AbstractScriptSession<?> provideAbstractScriptSession(
            final UpdateGraph updateGraph,
            final OperationInitializer operationInitializer) {
        return new NoLanguageDeephavenSession(
                updateGraph, operationInitializer, "non-script-session");
    }

    @Provides
    ScriptSession provideScriptSession(AbstractScriptSession<?> scriptSession) {
        return scriptSession;
    }

    @Provides
    @Singleton
    ScheduledExecutorService provideExecutorService() {
        return Executors.newScheduledThreadPool(1);
    }

    @Provides
    Scheduler provideScheduler(ScheduledExecutorService concurrentExecutor) {
        return new Scheduler.DelegatingImpl(
                Executors.newSingleThreadExecutor(),
                concurrentExecutor,
                Clock.system());
    }

    @Provides
    @Named("session.tokenExpireMs")
    long provideTokenExpireMs() {
        return 60_000_000;
    }

    @Provides
    @Named("http.port")
    int provideHttpPort() {
        return 0;// 'select first available'
    }

    @Provides
    @Named("grpc.maxInboundMessageSize")
    int provideMaxInboundMessageSize() {
        return 1024 * 1024;
    }

    @Provides
    AuthorizationProvider provideAuthorizationProvider(TestAuthorizationProvider provider) {
        return provider;
    }

    @Provides
    @Singleton
    TestAuthorizationProvider provideTestAuthorizationProvider() {
        return new TestAuthorizationProvider();
    }

    @Provides
    @Singleton
    static UpdateGraph provideUpdateGraph() {
        return ExecutionContext.getContext().getUpdateGraph();
    }

    @Provides
    @Singleton
    static OperationInitializer provideOperationInitializer() {
        return ExecutionContext.getContext().getOperationInitializer();
    }
}
