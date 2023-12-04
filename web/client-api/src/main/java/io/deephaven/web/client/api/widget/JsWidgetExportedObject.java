/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.widget;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.ServerObject;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.console.JsVariableDefinition;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsProperty;

/**
 * Represents a server-side object that may not yet have been fetched by the client. Does not memoize its result, so
 * fetch() should only be called once, and calling close() on this object will also close the result of the fetch.
 */
@TsInterface
@TsName(namespace = "dh", name = "WidgetExportedObject")
public class JsWidgetExportedObject implements ServerObject {
    private final WorkerConnection connection;

    private final TypedTicket ticket;

    /** Whether this exported object instance is the owner of the ticket or not. */
    private boolean isOwner = true;

    public JsWidgetExportedObject(WorkerConnection connection, TypedTicket ticket) {
        this.connection = connection;
        this.ticket = ticket;
    }

    @JsProperty
    public String getType() {
        return ticket.getType();
    }

    @Override
    public TypedTicket typedTicket() {
        TypedTicket typedTicket = new TypedTicket();
        typedTicket.setTicket(ticket.getTicket());
        typedTicket.setType(getType());
        return typedTicket;
    }

    public boolean isOwner() {
        return isOwner;
    }

    public Promise<?> fetch() {
        return fetch(false);
    }

    private void verifyIsOwner() {
        if (!isOwner) {
            throw new IllegalStateException("Exported object is no longer the owner of this ticket.");
        }
    }

    /**
     * Takes ownership of this exported object. Returns the ticket for this object.
     */
    public TypedTicket takeTicket() {
        verifyIsOwner();
        isOwner = false;
        return typedTicket();
    }

    /**
     * Fetches the object from the server.
     * 
     * @param takeOwnership Whether to take ownership of the object. Defaults to false.
     * @return a promise that resolves to the object
     */
    @JsMethod
    public Promise<?> fetch(@JsOptional Boolean takeOwnership) {
        if (takeOwnership == null) {
            takeOwnership = false;
        }
        verifyIsOwner();
        if (takeOwnership) {
            return this.connection.getObject(takeTicket());
        }
        return this.connection.getObject(
                new JsVariableDefinition(ticket.getType(), null, ticket.getTicket().getTicket_asB64(), null));
    }

    /**
     * Releases the server-side resources associated with this object, regardless of whether or not other client-side
     * objects exist that also use that object.
     */
    @JsMethod
    public void close() {
        verifyIsOwner();
        connection.releaseTicket(ticket.getTicket());
    }
}
