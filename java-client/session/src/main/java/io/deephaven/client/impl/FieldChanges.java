package io.deephaven.client.impl;

import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class FieldChanges {
    private final FieldsChangeUpdate changes;

    FieldChanges(FieldsChangeUpdate changes) {
        this.changes = Objects.requireNonNull(changes);
    }

    public boolean isEmpty() {
        return changes.getCreatedCount() == 0 && changes.getUpdatedCount() == 0 && changes.getRemovedCount() == 0;
    }

    public List<FieldInfo> created() {
        return changes.getCreatedList().stream().map(FieldInfo::new).collect(Collectors.toList());
    }

    public List<FieldInfo> updated() {
        return changes.getUpdatedList().stream().map(FieldInfo::new).collect(Collectors.toList());
    }

    public List<FieldInfo> removed() {
        return changes.getRemovedList().stream().map(FieldInfo::new).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return changes.toString();
    }
}
