/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import io.deephaven.auth.AuthContext;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.FlattenRequest;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.table.TableServicePrivilege;

import javax.inject.Inject;
import java.util.List;

public class FlattenTableGrpcImpl extends GrpcTableOperation<FlattenRequest> {

    @Inject
    public FlattenTableGrpcImpl() {
        super(BatchTableRequest.Operation::getFlatten, FlattenRequest::getResultId, FlattenRequest::getSourceId);
    }

    @Override
    public Table create(final AuthContext authContext,
            final FlattenRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        authContext.requirePrivilege(TableServicePrivilege.CAN_FLATTEN);
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);

        final Table parent = sourceTables.get(0).get();
        return parent.flatten();
    }
}
