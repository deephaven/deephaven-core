package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedMutableTable;
import io.deephaven.engine.table.impl.util.KeyedArrayBackedMutableTable;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest;
import io.deephaven.proto.flight.util.SchemaHelper;
import io.deephaven.server.session.SessionState;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flatbuf.Schema;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;

import static io.deephaven.proto.backplane.grpc.CreateInputTableRequest.InputTableKind.KindCase.KIND_NOT_SET;

@Singleton
public class CreateInputTableGrpcImpl extends GrpcTableOperation<CreateInputTableRequest> {

    private static final MultiDependencyFunction<CreateInputTableRequest> optionalSourceTable =
            (CreateInputTableRequest req) -> req.hasSourceTableId()
                    ? Collections.singletonList(req.getSourceTableId())
                    : Collections.emptyList();

    @Inject
    public CreateInputTableGrpcImpl() {
        super(BatchTableRequest.Operation::getCreateInputTable, CreateInputTableRequest::getResultId,
                optionalSourceTable);
    }

    @Override
    public void validateRequest(CreateInputTableRequest request) throws StatusRuntimeException {
        // ensure we have one of either schema or source table (protobuf will ensure we don't have both)
        if (!request.hasSchema() && !request.hasSourceTableId()) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "Must specify one of schema and source_table_id");
        }

        if (request.getKind().getKindCase() == null ||
                request.getKind().getKindCase() == KIND_NOT_SET) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "Unrecognized InputTableKind");
        }
    }

    @Override
    public Table create(CreateInputTableRequest request, List<SessionState.ExportObject<Table>> sourceTables) {
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
        switch (request.getKind().getKindCase()) {
            case IN_MEMORY_APPEND_ONLY:
                return AppendOnlyArrayBackedMutableTable.make(tableDefinitionFromSchema);
            case IN_MEMORY_KEY_BACKED:
                return KeyedArrayBackedMutableTable.make(tableDefinitionFromSchema,
                        request.getKind().getInMemoryKeyBacked().getKeyColumnsList()
                                .toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
            case KIND_NOT_SET:
            default:
                throw new IllegalStateException("Unsupported input table kind");
        }
    }
}
