package io.deephaven.grpc_api.table.ops;

import com.google.rpc.Code;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.utils.AppendOnlyArrayBackedMutableTable;
import io.deephaven.db.v2.utils.KeyedArrayBackedMutableTable;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.Schema;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;

@Singleton
public class CreateInputTableGrpcImpl extends GrpcTableOperation<CreateInputTableRequest> {

    private static final MultiDependencyFunction<CreateInputTableRequest> optionalSourceTable =
            (CreateInputTableRequest req) -> req.hasSourceTableId()
                    ? Collections.singletonList(req.getSourceTableId())
                    : Collections.emptyList();

    @Inject
    public CreateInputTableGrpcImpl() {
        super(BatchTableRequest.Operation::getCreateInputTable, CreateInputTableRequest::getResultId, optionalSourceTable);
    }

    @Override
    public void validateRequest(CreateInputTableRequest request) throws StatusRuntimeException {
        // ensure we have one of either schema or source table (protobuf will ensure we don't have both)
        if (!request.hasSchema() && !request.hasSourceTableId()) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Must specify one of schema and source_table_id");
        }

        if (request.getKind() == CreateInputTableRequest.InputTableKind.UNRECOGNIZED) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Unrecognized InputTableKind");
        }
    }

    @Override
    public Table create(CreateInputTableRequest request, List<SessionState.ExportObject<Table>> sourceTables) {
        TableDefinition tableDefinitionFromSchema;

        if (request.hasSchema()) {
            Message message = Message.getRootAsMessage(request.getSchema().asReadOnlyByteBuffer());
            if (message.headerType() != MessageHeader.Schema) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Must specify schema header in schema message");
            }
            tableDefinitionFromSchema = BarrageUtil.convertArrowSchema((Schema) message.header(new Schema())).tableDef;
        } else if (request.hasSourceTableId()) {
            Table sourceTable = sourceTables.get(0).get();
            tableDefinitionFromSchema = sourceTable.getDefinition();
        } else {
            throw new IllegalStateException("missing schema and source_table_id");
        }

        switch (request.getKind()) {
            case APPEND_ONLY:
                return AppendOnlyArrayBackedMutableTable.make(tableDefinitionFromSchema);
            case KEYED_ARRAY_BACKED:
                return KeyedArrayBackedMutableTable.make(tableDefinitionFromSchema, request.getKeyColumnsList().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
            default:
                throw new IllegalStateException("Unsupported input table kind");
        }
    }
}
