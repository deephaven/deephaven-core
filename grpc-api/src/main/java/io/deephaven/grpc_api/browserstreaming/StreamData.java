package io.deephaven.grpc_api.browserstreaming;

import io.deephaven.proto.backplane.grpc.Ticket;

public class StreamData {
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
