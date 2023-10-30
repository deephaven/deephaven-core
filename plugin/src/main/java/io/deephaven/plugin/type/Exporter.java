package io.deephaven.plugin.type;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;

/**
 * Utility to export objects while writing a payload to send to the client.
 */
public class Exporter {
    private final List<Object> references = new ArrayList<>();
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    /**
     * Gets the reference for {@code object} if it has already been created and {@code forceNew} is {@code false},
     * otherwise creates a new one. If {@code allowUnknownType} is {@code false}, and no type can be found, no reference
     * will be created. Uses reference-based equality.
     *
     * @deprecated Please use {@link #reference(Object)} instead - as of 0.27.0, the parameters allowUnknownType and
     *             forceNew can only be set to true going forward.
     *
     * @param object the object
     * @param allowUnknownType if an unknown-typed reference can be created
     * @param forceNew if a new reference should be created
     * @return the reference
     */
    @Deprecated(since = "0.27.0")
    public Optional<Reference> reference(Object object, boolean allowUnknownType, boolean forceNew) {
        return reference(object, allowUnknownType, forceNew, Object::equals);
    }

    /**
     * Gets the reference for {@code object} if it has already been created and {@code forceNew} is {@code false},
     * otherwise creates a new one. If {@code allowUnknownType} is {@code false}, and no type can be found, no reference
     * will be created.
     *
     * @deprecated Please use {@link #reference(Object)} instead - as of 0.27.0, the parameters allowUnknownType and
     *             forceNew can only be set to true going forward.
     *
     * @param object the object
     * @param allowUnknownType if an unknown-typed reference can be created
     * @param forceNew if a new reference should be created
     * @param equals the equals logic - unused.
     * @return the reference
     */
    @Deprecated(since = "0.27.0")
    public Optional<Reference> reference(Object object, boolean allowUnknownType, boolean forceNew,
            BiPredicate<Object, Object> equals) {
        if (!allowUnknownType) {
            throw new IllegalArgumentException("allowUnknownType must be true");
        }
        if (!forceNew) {
            throw new IllegalArgumentException("forceNew must be true");
        }
        int index = references.size();
        references.add(object);

        return Optional.of(() -> index);
    }

    public OutputStream outputStream() {
        return outputStream;
    }

    public ByteBuffer payload() {
        return ByteBuffer.wrap(outputStream.toByteArray());
    }

    public Object[] references() {
        return references.toArray();
    }

    /**
     * Creates a new reference for the provided object. A reference will be created and the object exported even if the
     * object has no corresponding plugin provided, granting the client the ability to reference the object on later
     * calls, and requiring that they release it when no longer needed.
     *
     * @param object the object
     * @return the reference
     */
    public Reference reference(Object object) {
        // noinspection OptionalGetWithoutIsPresent
        return reference(object, true, true).get();
    }

    /**
     * A reference.
     */
    public interface Reference {
        /**
         * The index, which is defined by the order in which references are created. May be used in the output stream to
         * refer to the reference from the client.
         *
         * @return the index
         */
        int index();

        /**
         * The type.
         *
         * @deprecated As of 0.27.0, this will always return empty.
         * @return the type, if present
         */
        @Deprecated(since = "0.27.0")
        default Optional<String> type() {
            return Optional.empty();
        }
    }
}
