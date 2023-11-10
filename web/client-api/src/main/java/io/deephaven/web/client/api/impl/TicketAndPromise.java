package io.deephaven.web.client.api.impl;

import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.shared.fu.JsFunction;

/**
 * Pair of ticket and the promise that indicates it has been resolved. Tickets are usable before they are resolved,
 * but to ensure that all operations completed successfully, the promise should be used to handle errors.
 */
public class TicketAndPromise<T> implements IThenable<T> {
    private final Ticket ticket;
    private final Promise<T> promise;
    private final WorkerConnection connection;
    private boolean released = false;

    public TicketAndPromise(Ticket ticket, Promise<T> promise, WorkerConnection connection) {
        this.ticket = ticket;
        this.promise = promise;
        this.connection = connection;
    }

    public TicketAndPromise(Ticket ticket, WorkerConnection connection) {
        this(ticket, (Promise<T>) Promise.resolve(ticket), connection);
    }

    public Promise<T> promise() {
        return promise;
    }

    public Ticket ticket() {
        return ticket;
    }

    @Override
    public <V> TicketAndPromise<V> then(ThenOnFulfilledCallbackFn<? super T, ? extends V> onFulfilled) {
        return new TicketAndPromise<>(ticket, promise.then(onFulfilled), connection);
    }

    /**
     * Rather than waiting for the original promise to succeed, lets the caller start a new call based
     * only on the original ticket.
     * @param racedCall
     * @return
     * @param <V>
     */
    public <V> TicketAndPromise<V> race(JsFunction<Ticket, IThenable<V>> racedCall) {
        IThenable<V> raced = racedCall.apply(ticket);
        return new TicketAndPromise<>(ticket, Promise.all(promise, raced).then(ignore -> raced), connection);
    }

    @Override
    public <V> IThenable<V> then(ThenOnFulfilledCallbackFn<? super T, ? extends V> onFulfilled, ThenOnRejectedCallbackFn<? extends V> onRejected) {
        return promise.then(onFulfilled, onRejected);
    }

    public void release() {
        if (!released) {
            // don't double-release, in cases where the same ticket is used for multiple parts of the request
            released = true;
            connection.releaseTicket(ticket);
        }
    }
}
