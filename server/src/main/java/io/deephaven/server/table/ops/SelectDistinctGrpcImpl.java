/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.SelectDistinctRequest;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionState;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Singleton
public class SelectDistinctGrpcImpl extends GrpcTableOperation<SelectDistinctRequest> {
    @Inject
    public SelectDistinctGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionSelectDistinct, BatchTableRequest.Operation::getSelectDistinct,
                SelectDistinctRequest::getResultId, SelectDistinctRequest::getSourceId);
    }

    @Override
    public Table create(final SelectDistinctRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);

        final Table parent = sourceTables.get(0).get();

        // explicitly disallow column expressions
        final Set<String> requestedMissing = new HashSet<>(request.getColumnNamesList());
        requestedMissing.removeAll(parent.getDefinition().getColumnNameMap().keySet());
        if (!requestedMissing.isEmpty()) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "column(s) not found: " + String.join(", ", requestedMissing));
        }

        return parent.getUpdateContext().apply(
                () -> parent.selectDistinct(request.getColumnNamesList().toArray(String[]::new)));
    }
}
