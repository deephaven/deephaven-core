package io.deephaven.uri;

import java.net.URI;
import java.util.List;

/**
 * A resolvable URI is a structured URI that may be resolvable to Deephaven resources.
 *
 * @see QueryScopeUri
 * @see ApplicationUri
 * @see FieldUri
 * @see RemoteUri
 * @see RawUri
 */
public interface ResolvableUri {

    /**
     * The URI.
     *
     * @return the URI.
     */
    URI toUri();

    /**
     * The scheme.
     *
     * @return the scheme
     */
    String scheme();

    /**
     * Relative path of the different parts, which includes the {@link #scheme() scheme} as the first part.
     *
     * @return the relative path of the parts
     */
    List<String> toParts();

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
     * @return the uri string
     */
    @Override
    String toString();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(QueryScopeUri queryScopeUri);

        void visit(ApplicationUri applicationUri);

        void visit(FieldUri fieldUri);

        void visit(RemoteUri remoteUri);

        void visit(URI uri);
    }
}
