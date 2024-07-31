//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable;
import io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest.InputTableKind.KindCase;
import io.deephaven.proto.flight.util.SchemaHelper;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionState;
import io.deephaven.stream.TablePublisher;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flatbuf.Schema;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.deephaven.proto.backplane.grpc.CreateInputTableRequest.InputTableKind.KindCase.KIND_NOT_SET;

@Singleton
public class CreateInputTableGrpcImpl extends GrpcTableOperation<CreateInputTableRequest> {

    private static final MultiDependencyFunction<CreateInputTableRequest> optionalSourceTable =
            (CreateInputTableRequest req) -> req.hasSourceTableId()
                    ? Collections.singletonList(req.getSourceTableId())
                    : Collections.emptyList();

    private static final AtomicInteger blinkTableCount = new AtomicInteger();

    @Inject
    public CreateInputTableGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionCreateInputTable, BatchTableRequest.Operation::getCreateInputTable,
                CreateInputTableRequest::getResultId, optionalSourceTable);
    }

    @Override
    public void validateRequest(CreateInputTableRequest request) throws StatusRuntimeException {
        // ensure we have one of either schema or source table (protobuf will ensure we don't have both)
        if (!request.hasSchema() && !request.hasSourceTableId()) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "Must specify one of schema and source_table_id");
        }

        if (request.getKind().getKindCase() == null ||
                request.getKind().getKindCase() == KIND_NOT_SET) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Unrecognized InputTableKind");
        }
    }

    @Override
    public Table create(final CreateInputTableRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        TableDefinition tableDefinitionFromSchema;

        if (request.hasSchema()) {
            Schema schema = SchemaHelper.flatbufSchema(request.getSchema().asReadOnlyByteBuffer());
            tableDefinitionFromSchema = BarrageUtil.convertArrowSchema(schema).tableDef;
        } else if (request.hasSourceTableId()) {
            Table sourceTable = sourceTables.get(0).get();
            tableDefinitionFromSchema = sourceTable.getDefinition();
        } else {
            throw new IllegalStateException("missing schema and source_table_id");
        }
        final Table table = create(request, tableDefinitionFromSchema);
        if (!table.hasAttribute(Table.INPUT_TABLE_ATTRIBUTE)) {
            throw new IllegalStateException(
                    String.format("Expected table to have attribute '%s'", Table.INPUT_TABLE_ATTRIBUTE));
        }
        return table;
    }

    private static Table create(CreateInputTableRequest request, TableDefinition tableDefinitionFromSchema) {
        final KindCase kindCase = request.getKind().getKindCase();
        switch (kindCase) {
            case IN_MEMORY_APPEND_ONLY:
                return AppendOnlyArrayBackedInputTable.make(tableDefinitionFromSchema);
            case IN_MEMORY_KEY_BACKED:
                return KeyedArrayBackedInputTable.make(tableDefinitionFromSchema,
                        request.getKind().getInMemoryKeyBacked().getKeyColumnsList()
                                .toArray(String[]::new));
            case BLINK:
                final String name =
                        CreateInputTableGrpcImpl.class.getSimpleName() + ".BLINK-" + blinkTableCount.getAndIncrement();
                return TablePublisher.of(name, tableDefinitionFromSchema, null, null).inputTable();
            case KIND_NOT_SET:
            default:
                throw new IllegalStateException("Unsupported input table kind: " + kindCase);
        }
    }
}
