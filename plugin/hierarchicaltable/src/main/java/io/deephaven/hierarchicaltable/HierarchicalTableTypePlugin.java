/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.hierarchicaltable;

import com.google.auto.service.AutoService;
import com.google.protobuf.ByteString;
import io.deephaven.api.ColumnName;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.proto.backplane.grpc.HierarchicalTableDescriptor;
import io.deephaven.proto.backplane.grpc.RollupDescriptorDetails;
import io.deephaven.proto.backplane.grpc.RollupNodeType;
import io.deephaven.proto.backplane.grpc.TreeDescriptorDetails;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.stream.Collectors;

/**
 * An object type named {@value #NAME} of java class type {@link HierarchicalTable}.
 */
@AutoService(ObjectType.class)
public class HierarchicalTableTypePlugin extends ObjectTypeBase {

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

        final HierarchicalTableDescriptor.Builder builder = HierarchicalTableDescriptor.newBuilder()
                .setRowDepthColumn(hierarchicalTable.getRowDepthColumn().name())
                .setRowExpandedColumn(hierarchicalTable.getRowExpandedColumn().name());

        if (hierarchicalTable instanceof RollupTable) {
            final RollupTable rollupTable = (RollupTable) hierarchicalTable;
            builder.addAllExpandByColumns(rollupTable.getGroupByColumns().stream()
                    .map(ColumnName::name)
                    .collect(Collectors.toList()));
            builder.setRollup(RollupDescriptorDetails.newBuilder()
                    .setLeafNodeType(rollupTable.includesConstituents()
                            ? RollupNodeType.CONSTITUENT
                            : RollupNodeType.AGGREGATED)
                    .addAllOutputInputColumnPairs(rollupTable.getColumnPairs().stream()
                            .map(cnp -> cnp.output().equals(cnp.input())
                                    ? cnp.output().name()
                                    : cnp.output().name() + '=' + cnp.input().name())
                            .collect(Collectors.toList()))
                    .build());
        } else if (hierarchicalTable instanceof TreeTable) {
            final TreeTable treeTable = (TreeTable) hierarchicalTable;
            builder.addExpandByColumns(treeTable.getIdentifierColumn().name());
            builder.setTree(TreeDescriptorDetails.getDefaultInstance());
        } else {
            throw new IllegalArgumentException("Unknown HierarchicalTable type");
        }

        final ByteString snapshotDefinitionSchema = BarrageUtil.schemaBytesFromTable(
                hierarchicalTable.getSnapshotDefinition(), hierarchicalTable.getAttributes());
        builder.setSnapshotDefinitionSchema(snapshotDefinitionSchema);

        final HierarchicalTableDescriptor result = builder.build();
        result.writeTo(out);
    }
}
