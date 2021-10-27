package io.deephaven.uri;

import java.net.URI;

/**
 * A structured URI is an object which can be represented in serialized form as a {@link URI}; and subsequently, can be
 * deserialized back into the same object.
 *
 * @see DeephavenUri
 * @see CustomUri
 */
public interface StructuredUri {

    /**
     * The URI.
     *
     * @return the URI.
     */
    URI toURI();

    /**
     * Wraps up {@code this} URI as a {@link RemoteUri remote URI}.
     *
     * @param target the target
     * @return the remote URI
     */
    RemoteUri target(DeephavenTarget target);

    /**
     * The URI string.
     *
     * @return the URI string
     */
    @Override
    String toString();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(QueryScopeUri queryScopeUri);

        void visit(ApplicationUri applicationUri);

        void visit(FieldUri fieldUri);

        void visit(RemoteUri remoteUri);

        void visit(URI customUri);
    }
}
