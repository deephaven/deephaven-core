/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.hierarchicaltable;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeClassBase;
import io.deephaven.proto.backplane.grpc.HierarchicalTableViewDescriptor;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An object type named {@value NAME} of java class type {@link HierarchicalTable}.
 */
@AutoService(ObjectType.class)
public class HierarchicalTableViewTypePlugin extends ObjectTypeClassBase<HierarchicalTable> {

    public static final String NAME = "HierarchicalTableView";

    public HierarchicalTableViewTypePlugin() {
        super(NAME, HierarchicalTable.class);
    }

    @Override
    public void writeToImpl(Exporter exporter, HierarchicalTable object, OutputStream out) throws IOException {
        HierarchicalTableViewDescriptor.newBuilder();

        // TODO-RWC: IMPLEMENT ME
        // PartitionedTable partitionedTable = (PartitionedTable) object;
        // exporter.reference(partitionedTable.table(), false, true);
        //
        // // Send Schema wrapped in Message
        // ByteString schemaWrappedInMessage =
        // BarrageUtil.schemaBytesFromTable(partitionedTable.constituentDefinition(), Collections.emptyMap());
        //
        // PartitionedTableDescriptor result = PartitionedTableDescriptor.newBuilder()
        // .addAllKeyColumnNames(partitionedTable.keyColumnNames())
        // .setUniqueKeys(partitionedTable.uniqueKeys())
        // .setConstituentDefinitionSchema(schemaWrappedInMessage)
        // .setConstituentColumnName(partitionedTable.constituentColumnName())
        // .setConstituentChangesPermitted(partitionedTable.constituentChangesPermitted())
        // .build();
        // result.writeTo(out);
    }
}
