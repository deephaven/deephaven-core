//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples.tools;

import io.deephaven.api.TableOperations;
import io.deephaven.client.examples.AuthenticationOptions;
import io.deephaven.client.examples.BatchOrSerialOptions;
import io.deephaven.client.examples.ConnectOptions;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Stress tool: builds a deeply nested query of alternating head and tail operations, to exercise how batch and serial
 * modes handle a long chain, then reads the result back over Arrow Flight and reports the time taken.
 */
@Command(name = "deep-query", mixinStandardHelpOptions = true,
        description = "Executes a deep query",
        version = "0.1.0")
class DeepQuery implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @ArgGroup(exclusive = true)
    BatchOrSerialOptions mode;

    @Option(names = {"-c", "--count"}, description = "The amount of heads/tails",
            defaultValue = "256")
    long count;

    /**
     * The table logic, written against {@link TableOperations} so it can run against any implementation.
     */
    <T extends TableOperations<T, T>> T create(TableCreator<T> creation) {
        T base = creation.of(TableSpec.empty(count * 2)).view("I=i");
        for (long i = 0; i < count; ++i) {
            base = base.head(2 * count - 2 * i);
            base = base.tail(2 * count - 2 * i - 1);
        }
        return base;
    }

    @Override
    public Void call() throws Exception {
        final BufferAllocator allocator = new RootAllocator();
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        final FlightSessionFactoryConfig.Factory factory = FlightSessionFactoryConfig.builder()
                .clientConfig(ConnectOptions.options(connectOptions).config())
                .sessionConfig(AuthenticationOptions.sessionConfig(authenticationOptions))
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
        try (final FlightSession flight = factory.newFlightSession()) {
            final TableHandleManager manager = BatchOrSerialOptions.manager(mode, flight.session());
            final long start = System.nanoTime();
            try (
                    final TableHandle handle = manager.executeLogic((TableCreationLogic) this::create);
                    final FlightStream stream = flight.stream(handle)) {
                System.out.println(stream.getSchema());
                while (stream.next()) {
                    System.out.println(stream.getRoot().contentToTSVString());
                }
            }
            System.out.printf("%s duration%n", Duration.ofNanos(System.nanoTime() - start));
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new DeepQuery()).execute(args));
    }
}
