package io.deephaven.client.impl;

import io.deephaven.proto.backplane.grpc.TypedTicket;

import java.util.Objects;
import java.util.Optional;

public final class FieldInfo {

    private final io.deephaven.proto.backplane.grpc.FieldInfo fieldInfo;

    FieldInfo(io.deephaven.proto.backplane.grpc.FieldInfo fieldInfo) {
        this.fieldInfo = Objects.requireNonNull(fieldInfo);
    }

    public TypedTicket getTicket() {
        return fieldInfo.getTicket();
    }

    public String type() {
        return fieldInfo.getTicket().getType();
    }

    public TicketId ticket() {
        return new TicketId(fieldInfo.getTicket().toByteArray());
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
