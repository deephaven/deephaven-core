package io.deephaven.base.system;

import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class StandardStreamState {

    private final Set<StandardStreamReceiver> receivers;
    private final AtomicBoolean initialized;

    public StandardStreamState(Set<StandardStreamReceiver> receivers) {
        this.receivers = Objects.requireNonNull(receivers);
        this.initialized = new AtomicBoolean(false);
    }

    public void setupRedirection() throws UnsupportedEncodingException {
        if (!initialized.compareAndSet(false, true)) {
            throw new IllegalStateException(
                "May only call StandardStreamState#setupRedirection once");
        }

        // get all of the out sinks
        List<OutputStream> outReceivers = receivers.stream()
            .map(StandardStreamReceiver::receiveOut)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

        // get all of the err sinks
        List<OutputStream> errReceivers = receivers.stream()
            .map(StandardStreamReceiver::receiveErr)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

        if (!outReceivers.isEmpty()) {
            PrintStream out = adapt(outReceivers);
            if (out != System.out) {
                System.setOut(out);
                out.println("# New System.out configured");
            }
        }

        if (!errReceivers.isEmpty()) {
            PrintStream err = adapt(errReceivers);
            if (err != System.err) {
                System.setErr(err);
                err.println("# New System.err configured");
            }
        }
    }

    private static PrintStream adapt(List<OutputStream> outputStreams)
        throws UnsupportedEncodingException {
        // TODO (core#88): Figure out appropriate stdout / LogBuffer encoding
        if (outputStreams.size() == 1) {
            OutputStream out = outputStreams.get(0);
            if (out instanceof PrintStream) {
                return (PrintStream) out;
            }
            return new PrintStream(out, true, "ISO-8859-1");
        }
        return new PrintStream(MultipleOutputStreams.of(outputStreams), true, "ISO-8859-1");
    }
}


