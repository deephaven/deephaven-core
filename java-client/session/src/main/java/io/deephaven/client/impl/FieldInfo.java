package io.deephaven.client.impl;

import java.util.Objects;
import java.util.Optional;

public final class FieldInfo {

    private final io.deephaven.proto.backplane.grpc.FieldInfo fieldInfo;

    FieldInfo(io.deephaven.proto.backplane.grpc.FieldInfo fieldInfo) {
        this.fieldInfo = Objects.requireNonNull(fieldInfo);
    }

    public Optional<String> type() {
        if (fieldInfo.getTypedTicket().getType().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(fieldInfo.getTypedTicket().getType());
    }

    public TicketId ticket() {
        return new TicketId(fieldInfo.getTypedTicket().getTicket());
    }

    public String name() {
        return fieldInfo.getFieldName();
    }

    public Optional<String> description() {
        return fieldInfo.getFieldDescription().isEmpty() ? Optional.empty()
                : Optional.of(fieldInfo.getFieldDescription());
    }

    public String applicationId() {
        return fieldInfo.getApplicationId();
    }

    public String applicationName() {
        return fieldInfo.getApplicationName();
    }

    @Override
    public String toString() {
        return fieldInfo.toString();
    }
}
