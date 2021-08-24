package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import com.google.rpc.Code;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.web.shared.data.LocalDate;
import io.deephaven.web.shared.data.LocalTime;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class EmptyTableGrpcImpl extends GrpcTableOperation<EmptyTableRequest> {

    @Inject()
    public EmptyTableGrpcImpl() {
        super(BatchTableRequest.Operation::getEmptyTable, EmptyTableRequest::getResultId);
    }

    @Override
    public void validateRequest(final EmptyTableRequest request) throws StatusRuntimeException {
        if (request.getSize() < 0) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                "Size must be greater than zero");
        }
    }

    @Override
    public Table create(final EmptyTableRequest request,
        final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 0);

        return TableTools.emptyTable(request.getSize());
    }
}
