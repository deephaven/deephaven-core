//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.ClientData;
import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.HasTypedTicket;
import io.deephaven.client.impl.ObjectService.Bidirectional;
import io.deephaven.client.impl.ObjectService.MessageStream;
import io.deephaven.client.impl.ServerData;
import io.deephaven.client.impl.ServerObject;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.TableObject;
import io.deephaven.client.impl.TypedTicket;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Prints out the returned messages from a {@link Session#bidirectional(HasTypedTicket) bidirectional message stream}.
 * This is suitable for object type protocols that send one response per request. Additionally,
 * {@link Session#publish(String, HasTicketId) publishes} all {@link TableObject table objects} with the name template
 * "my_table_{ix}".
 *
 * <p>
 * For example, here is an example using this application with the "Echo" object type plugin.
 *
 * <pre>
 * $ echo -n "hello, world" > /tmp/hello.txt
 * $ message-stream-send-receive /tmp/hello.txt Echo:s/my_echo Table:s/my_table Figure:s/my_figure
 * data:
 *
 * data: hello, world
 * export: Table:export/-1
 * export: Figure:export/-2
 *
 * onClose
 *
 * Releasing non-Table ServerObjects...
 * Released Figure:export/-2
 *
 * Publishing and releasing TableObjects...
 * Published Table:export/-1 as my_table_0
 * Released Table:export/-1
 * </pre>
 *
 * The above assumes there is an Echo object (io.deephaven.plugin.type.EchoObjectType.INSTANCE) in the global scope
 * named "my_echo", a Table object in the global scope named "my_table", and a Figure object in the global scope named
 * "my_figure". The application connects to the "my_echo" object, and then proceeds to send a message with the data
 * "hello, world" and typed tickets for "my_table" and "my_figure" to the server. The response is echoed back and
 * printed out by this application. The Table:export/-1, which was originally referenced via "my_table", will be
 * published as "my_table_0".
 */
@Command(name = "message-stream-send-receive", mixinStandardHelpOptions = true,
        description = "Message stream send and receive", version = "0.1.0")
class MessageStreamSendReceive extends SingleSessionExampleBase {

    @Option(names = {"--count"}, description = "The number of messages to send, defaults to 1", defaultValue = "1")
    int count = 1;

    @Parameters(index = "0", description = "The input file to send.")
    Path inputFile;

    @Parameters(index = "1", description = "The type ticket to connect to.", converter = TypedTicketConverter.class)
    TypedTicket typedTicket;

    @Parameters(index = "2..", arity = "0*", description = "The type tickets to send.",
            converter = TypedTicketConverter.class)
    List<TypedTicket> typedTickets;

    @Override
    protected void execute(Session session) throws Exception {
        final CountDownLatch onConnectData = new CountDownLatch(1);
        final CountDownLatch onData = new CountDownLatch(count);
        final List<TableObject> tableObjects = new ArrayList<>();
        final List<ServerObject> otherObjects = new ArrayList<>();;
        try (final Bidirectional bidirectional = session.bidirectional(typedTicket).get()) {
            final MessageStream<ServerData> fromServer = new MessageStream<>() {
                @Override
                public void onData(ServerData serverData) {
                    try {
                        System.out.println("data: " + byteBufferToString(serverData.data().slice()));
                        for (ServerObject export : serverData.exports()) {
                            System.out.println("export: " + export);
                            if (export instanceof TableObject) {
                                tableObjects.add((TableObject) export);
                            } else {
                                otherObjects.add(export);
                            }
                        }
                        System.out.println();
                    } finally {
                        if (onConnectData.getCount() == 1) {
                            onConnectData.countDown();
                        } else {
                            onData.countDown();
                        }
                    }
                }

                @Override
                public void onClose() {
                    final long remaining = onData.getCount();
                    if (remaining == 0) {
                        System.out.println("onClose");
                        System.out.println();
                        return;
                    }
                    if (onConnectData.getCount() == 1) {
                        System.out.println(
                                "Didn't receive an initial message - is the typedTicket to connect to correct?");
                        onConnectData.countDown();
                        for (long i = 0; i < remaining; ++i) {
                            onData.countDown();
                        }
                    } else {
                        System.out.println("onClose was early, expected " + remaining + " more messages");
                        for (long i = 0; i < remaining; ++i) {
                            onData.countDown();
                        }
                    }
                }
            };

            // Connect to the server, await the first message.
            final MessageStream<ClientData> toServer = bidirectional.connect(fromServer);
            onConnectData.await();

            // Send the messages
            final ClientData msg = new ClientData(ByteBuffer.wrap(Files.readAllBytes(inputFile)),
                    typedTickets == null ? Collections.emptyList() : typedTickets);
            for (int i = 0; i < count; ++i) {
                toServer.onData(msg);
            }

            // Wait for all of the messages to arrive
            onData.await();

            // Notify server we are done
            toServer.onClose();
        }

        // Wait for all of the non-table objects to be released
        System.out.println("Releasing non-Table ServerObjects...");
        waitAll(otherObjects.stream().map(MessageStreamSendReceive::releaseAndPrint).collect(Collectors.toList()));
        System.out.println();

        // Publish all table objects to named tables
        System.out.println("Publishing and releasing TableObjects...");
        final List<CompletableFuture<Void>> publishReleasePrint = new ArrayList<>(tableObjects.size());
        int i = 0;
        for (TableObject tableObject : tableObjects) {
            final String name = "my_table_" + (i++);
            publishReleasePrint
                    .add(publishAndPrint(session, name, tableObject).thenCompose(x -> releaseAndPrint(tableObject)));
        }

        // Wait for all of the table objects publish / release to be done
        waitAll(publishReleasePrint);
    }

    private static CompletableFuture<Void> publishAndPrint(Session session, String name, TableObject tableObject) {
        return session
                .publish(name, tableObject)
                .thenRun(() -> System.out.println("Published " + tableObject + " as " + name));
    }

    private static CompletableFuture<Void> releaseAndPrint(ServerObject serverObject) {
        return serverObject.release().thenRun(() -> System.out.println("Released " + serverObject));
    }

    private static void waitAll(List<? extends CompletableFuture<?>> futures)
            throws ExecutionException, InterruptedException {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    }

    private static String byteBufferToString(ByteBuffer bb) {
        final int remaining = bb.remaining();
        final byte[] bytes = new byte[remaining];
        bb.slice().get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new MessageStreamSendReceive()).execute(args);
        System.exit(execute);
    }
}
