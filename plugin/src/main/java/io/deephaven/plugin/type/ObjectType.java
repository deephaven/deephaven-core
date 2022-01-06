package io.deephaven.plugin.type;

import io.deephaven.plugin.type.Exporter.Reference;

import java.io.IOException;
import java.io.OutputStream;

public interface ObjectType {

    /**
     * The name of the object type.
     *
     * @return the name
     */
    String name();

    /**
     * Returns true if, and only if, the {@code object} is compatible with {@code this} object type.
     *
     * @param object the object
     * @return true if the {@code object} is compatible
     */
    boolean isType(Object object);

    /**
     * Serializes {@code object} into {@code out}. Must only be called with compatible objects, see
     * {@link #isType(Object)}. If the serialized {@code object} references another server side object(s), the other
     * server side object(s) should be referenced with the {@code exporter}, and the {@link Reference#id()} serialized
     * as appropriate.
     *
     * <p>
     * Note: the implementation should not hold onto references, or create references outside the calling thread.
     *
     * @param exporter the exporter
     * @param object the (compatible) object
     * @param out the output stream
     * @throws IOException if an IO exception occurs
     */
    void writeTo(Exporter exporter, Object object, OutputStream out) throws IOException;
}
