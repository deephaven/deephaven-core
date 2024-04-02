//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.partitionedtable;

import com.google.auto.service.AutoService;
import com.google.protobuf.ByteString;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.plugin.type.Exporter;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.proto.backplane.grpc.PartitionedTableDescriptor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;

/**
 * Protobuf serialization of all PartitionedTable subtypes. This sends the basic metadata of the partitioned table, and
 * a ticket to the underlying table that tracks the keys and the actual table objects.
 */
@AutoService(ObjectType.class)
public class PartitionedTableTypePlugin extends ObjectTypeBase.FetchOnly {

    @Override
    public String name() {
        return "PartitionedTable";
    }

    @Override
    public boolean isType(Object object) {
        return object instanceof PartitionedTable;
    }

    @Override
    public void writeCompatibleObjectTo(Exporter exporter, Object object, OutputStream out) throws IOException {
        PartitionedTable partitionedTable = (PartitionedTable) object;
        exporter.reference(partitionedTable.table());

        // Send Schema wrapped in Message
        ByteString schemaWrappedInMessage = BarrageUtil.schemaBytesFromTableDefinition(
                partitionedTable.constituentDefinition(),
                Collections.emptyMap(),
                false);

        PartitionedTableDescriptor result = PartitionedTableDescriptor.newBuilder()
                .addAllKeyColumnNames(partitionedTable.keyColumnNames())
                .setUniqueKeys(partitionedTable.uniqueKeys())
                .setConstituentDefinitionSchema(schemaWrappedInMessage)
                .setConstituentColumnName(partitionedTable.constituentColumnName())
                .setConstituentChangesPermitted(partitionedTable.constituentChangesPermitted())
                .build();
        result.writeTo(out);
    }
}
