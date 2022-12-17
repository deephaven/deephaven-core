/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.hierarchicaltable;

import com.google.auto.service.AutoService;
import com.google.protobuf.ByteString;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.proto.backplane.grpc.HierarchicalTableViewDescriptor;
import io.deephaven.proto.backplane.grpc.RollupDescriptorDetails;
import io.deephaven.proto.backplane.grpc.TreeDescriptorDetails;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

/**
 * } An object type named &quot;HierarchicalTable&quot; of java class type {@link HierarchicalTable}.
 */
@AutoService(ObjectType.class)
public class HierarchicalTableTypePlugin extends ObjectTypeBase {

    @Override
    public String name() {
        return "HierarchicalTable";
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
        exporter.reference(hierarchicalTable.getSource(), false, true);
        exporter.reference(null /* default view */, true, true);

        final HierarchicalTableViewDescriptor.Builder builder = HierarchicalTableViewDescriptor.newBuilder();

//        final ByteString schemaWrappedInMessage =
//                BarrageUtil.schemaBytesFromTable(partitionedTable.constituentDefinition(), Collections.emptyMap());

        if (hierarchicalTable instanceof RollupTable) {
            builder.setRollup(RollupDescriptorDetails.newBuilder()
                    .build());
        } else if (hierarchicalTable instanceof TreeTable) {
            builder.setTree(TreeDescriptorDetails.getDefaultInstance());
        } else {
            // panic
        }
        builder.build();

        final HierarchicalTableViewDescriptor result = builder.build();
        result.writeTo(out);


    }

    private HierarchicalTableView makeDefaultView() {
        return null;
    }
}
