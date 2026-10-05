//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples.tools;

import io.deephaven.client.examples.AuthenticationOptions;
import io.deephaven.client.examples.BatchOrSerialOptions;
import io.deephaven.client.examples.ConnectOptions;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactoryConfig;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.client.impl.TableService;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Protocol tool: shows that a stateful table service refuses to execute a second table derived from a non-deterministic
 * parent. The parent {@code R=random()} was already evaluated for the first child, so the second cannot share it, and
 * the client rejects the request before sending it.
 */
@Command(name = "tainted", mixinStandardHelpOptions = true,
        description = "Try to execute an unreferenceable table", version = "0.1.0")
class UnreferenceableTableExample implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @ArgGroup(exclusive = true)
    BatchOrSerialOptions mode;

    @Override
    public Void call() throws Exception {
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        final SessionFactoryConfig.Factory factory = SessionFactoryConfig.builder()
                .clientConfig(ConnectOptions.options(connectOptions).config())
                .sessionConfig(AuthenticationOptions.sessionConfig(authenticationOptions))
                .scheduler(scheduler)
                .build()
                .factory();
        try (final Session session = factory.newSession()) {
            final TableSpec r = TableSpec.empty(10).select("R=random()");
            final TableSpec rPlusOne = r.view("PlusOne=R + 1");
            final TableSpec rMinusOne = r.view("PlusOne=R - 1");
            final TableService statefulTableService = session.newStatefulTableService();
            final TableHandleManager manager = BatchOrSerialOptions.manager(mode, statefulTableService);
            // noinspection unused
            try (
                    final TableHandle hPlusOne = manager.execute(rPlusOne);
                    // this should throw an error
                    final TableHandle hMinusOne = manager.execute(rMinusOne)) {
                throw new RuntimeException("Expected an \"unreferenceable table\" exception");
            } catch (IllegalArgumentException e) {
                System.out.println("Expected");
                e.printStackTrace(System.out);
            }
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new UnreferenceableTableExample()).execute(args));
    }
}
