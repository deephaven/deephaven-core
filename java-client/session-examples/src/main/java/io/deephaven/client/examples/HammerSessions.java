package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;

@Command(name = "hammer-sessions", mixinStandardHelpOptions = true,
        description = "Create a lot of sessions as fast as possible", version = "0.1.0")
public class HammerSessions extends SessionExampleBase {

    @Parameters(arity = "1", paramLabel = "OUTSTANDING",
            description = "Maximum number of outstanding sessions.")
    int outstanding;

    @Parameters(arity = "1", paramLabel = "TOTAL",
            description = "The total number of sessions to create.")
    int total;

    @Override
    protected void execute(SessionFactory factory) throws Exception {
        final LongAdder failed = new LongAdder();
        Semaphore semaphore = new Semaphore(outstanding, false);

        final long start = System.nanoTime();
        for (long i = 0; i < total; ++i) {
            if (i % (outstanding / 3) == 0) {
                System.out.printf("Started %d, outstanding %d%n", i, semaphore.availablePermits());
            }
            semaphore.acquire();
            factory.newSessionFuture()
                    .whenComplete((BiConsumer<Session, Throwable>) (session, throwable) -> {
                        if (throwable != null) {
                            throwable.printStackTrace();
                            failed.add(1);
                        }
                        if (session != null) {
                            session.closeFuture().whenComplete((x, y) -> semaphore.release());
                        } else {
                            semaphore.release();
                        }
                    });
        }

        semaphore.acquire(outstanding);
        final long end = System.nanoTime();

        long f = failed.longValue();
        System.out.printf("%d succeeded, %d failed, avg duration %s%n", total - f, f,
                Duration.ofNanos(end - start).dividedBy(total));
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new HammerSessions()).execute(args);
        System.exit(execute);
    }
}
