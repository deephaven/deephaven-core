//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SharedId;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.TimeTable;
import picocli.CommandLine;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@CommandLine.Command(name = "create-shared-id", mixinStandardHelpOptions = true,
        description = "Exports a time table to a random shared id", version = "0.1.0")
class CreateSharedId extends SingleSessionExampleBase {

    @CommandLine.ArgGroup(exclusive = false)
    SharedField destination;

    @CommandLine.Option(names = {"--duration"},
            description = "How long to keep the shared id published before exiting, for example PT10S; "
                    + "unlimited if unset")
    Duration duration;

    @Override
    protected void execute(Session session) throws Exception {
        final SharedId sharedId = destination != null ? destination.sharedId() : SharedId.newRandom();
        final TableHandle timeTable = session.execute(TimeTable.of(Duration.ofSeconds(1)));
        session.publish(sharedId, timeTable).get();

        System.out.println("shared id: " + sharedId.asHexString());
        System.out.println();

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(latch::countDown));
        if (duration == null) {
            System.out.println("ctrl-C to kill");
            latch.await();
        } else {
            System.out.println("holding for " + duration);
            latch.await(duration.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new CreateSharedId()).execute(args);
        System.exit(execute);
    }
}
