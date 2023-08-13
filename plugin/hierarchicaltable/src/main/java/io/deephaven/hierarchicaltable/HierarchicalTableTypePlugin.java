/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.hierarchicaltable;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.HierarchicalTableSchemaUtil;
import io.deephaven.plugin.type.Exporter;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.proto.backplane.grpc.HierarchicalTableDescriptor;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An object type named {@value #NAME} of java class type {@link HierarchicalTable}.
 */
@AutoService(ObjectType.class)
public class HierarchicalTableTypePlugin extends ObjectTypeBase.FetchOnly {

    private static final String NAME = "HierarchicalTable";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean isType(final Object object) {
        return object instanceof HierarchicalTable;
    }

    @Override
    public void writeCompatibleObjectTo(
            @NotNull final Exporter exporter,
            @NotNull final Object object,
            @NotNull final OutputStream out) throws IOException {
        final HierarchicalTable<?> hierarchicalTable = (HierarchicalTable<?>) object;

        final HierarchicalTableDescriptor result = HierarchicalTableDescriptor.newBuilder()
                .setSnapshotSchema(BarrageUtil.schemaBytes(
                        fbb -> HierarchicalTableSchemaUtil.makeSchemaPayload(fbb, hierarchicalTable)))
                .build();

        result.writeTo(out);
    }
}
