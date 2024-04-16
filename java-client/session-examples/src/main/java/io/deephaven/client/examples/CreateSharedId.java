//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SharedId;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.TimeTable;
import picocli.CommandLine;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

@CommandLine.Command(name = "create-shared-id", mixinStandardHelpOptions = true,
        description = "Exports a time table to a random shared id", version = "0.1.0")
class CreateSharedId extends SingleSessionExampleBase {

    @CommandLine.ArgGroup(exclusive = false)
    SharedField destination;

    @Override
    protected void execute(Session session) throws Exception {
        final SharedId sharedId = destination != null ? destination.sharedId() : SharedId.newRandom();
        final TableHandle timeTable = session.execute(TimeTable.of(Duration.ofSeconds(1)));
        session.publish(sharedId, timeTable).get();

        System.out.println("shared id: " + sharedId.asHexString());
        System.out.println();
        System.out.println("ctrl-C to kill");

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(latch::countDown));
        latch.await();
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new CreateSharedId()).execute(args);
        System.exit(execute);
    }
}
