package io.deephaven.plugin.type;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;

class ExporterImpl implements ObjectType.Exporter {
    private final List<Object> references = new ArrayList<>();
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    @Override
    public Optional<Reference> reference(Object object, boolean allowUnknownType, boolean forceNew) {
        return reference(object, allowUnknownType, forceNew, Object::equals);
    }

    @Override
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

        return Optional.of(new Reference() {
            @Override
            public int index() {
                return index;
            }

            @Override
            public Optional<String> type() {
                return Optional.empty();
            }
        });
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
}
