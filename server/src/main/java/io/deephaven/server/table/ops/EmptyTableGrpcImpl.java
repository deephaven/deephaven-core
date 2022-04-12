package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.server.session.SessionState;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class EmptyTableGrpcImpl extends GrpcTableOperation<EmptyTableRequest> {

    @Inject()
    public EmptyTableGrpcImpl() {
        super(BatchTableRequest.Operation::getEmptyTable, EmptyTableRequest::getResultId);
    }

    @Override
    public void validateRequest(final EmptyTableRequest request) throws StatusRuntimeException {
        if (request.getSize() < 0) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Size must be greater than zero");
        }
    }

    @Override
    public Table create(final EmptyTableRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 0);

        return TableTools.emptyTable(request.getSize());
    }
}
