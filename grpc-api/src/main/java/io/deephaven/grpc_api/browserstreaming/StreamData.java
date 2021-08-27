package io.deephaven.grpc_api.browserstreaming;

import io.deephaven.proto.backplane.grpc.Ticket;
import io.grpc.Context;

public class StreamData {
    /**
     * Provided access to the emulated stream metadata, if any.
     */
    public static final Context.Key<StreamData> STREAM_DATA_KEY = Context.key("stream-data");

    private final Ticket rpcTicket;
    private final int sequence;
    private final boolean halfClose;

    public StreamData(Ticket rpcTicket, int sequence, boolean halfClose) {
        this.rpcTicket = rpcTicket;
        this.sequence = sequence;
        this.halfClose = halfClose;
    }

    public Ticket getRpcTicket() {
        return rpcTicket;
    }

    public boolean isHalfClose() {
        return halfClose;
    }

    public int getSequence() {
        return sequence;
    }
}
