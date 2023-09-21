/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.examples;

import io.deephaven.client.impl.ClientData;
import io.deephaven.client.impl.HasTypedTicket;
import io.deephaven.client.impl.ObjectService.Bidirectional;
import io.deephaven.client.impl.ObjectService.MessageStream;
import io.deephaven.client.impl.ServerData;
import io.deephaven.client.impl.ServerObject;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.TypedTicket;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Prints out the returned messages from a {@link Session#bidirectional(HasTypedTicket) bidirectional message stream}.
 * This is suitable for object type protocols that send one response per request.
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
 * </pre>
 *
 * The above assumes there is an Echo object (io.deephaven.plugin.type.EchoObjectType.INSTANCE) in the global scope
 * named "my_echo", a Table object in the global scope named "my_table", and a Figure object in the global scope named
 * "my_figure". The application connects to the "my_echo" object, and then proceeds to send a message with the data
 * "hello, world" and typed tickets for "my_table" and "my_figure" to the server. The response is echoed back and
 * printed out by this application.
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
        try (final Bidirectional bidirectional = session.bidirectional(typedTicket).get()) {
            final MessageStream<ServerData> fromServer = new MessageStream<>() {
                @Override
                public void onData(ServerData serverData) {
                    try {
                        System.out.println("data: " + byteBufferToString(serverData.data().slice()));
                        for (ServerObject export : serverData.exports()) {
                            System.out.println("export: " + export);
                        }
                        System.out.println();
                    } finally {
                        serverData.close();
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
            onData.await();

            toServer.onClose();
        }
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
