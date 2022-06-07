package io.deephaven.partitionedtable;

import com.google.auto.service.AutoService;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.plugin.type.ObjectTypeClassBase;
import io.deephaven.proto.backplane.grpc.PartitionedTableDescriptor;
import io.deephaven.proto.flight.util.MessageHelper;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;

/**
 * Protobuf serialization of all PartitionedTable subtypes. This sends the basic metadata of the partitioned
 * table, and a ticket to the underlying table that tracks the keys and the actual table objects.
 */
@AutoService(ObjectType.class)
public class PartitionedTableTypePlugin extends ObjectTypeBase {

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
        exporter.reference(partitionedTable.table(), false, true);

        // Send Schema wrapped in Message
        ByteString schemaWrappedInMessage = BarrageUtil.schemaBytesFromTable(partitionedTable.constituentDefinition(), Collections.emptyMap());

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
