/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.MetaTableRequest;
import io.deephaven.server.session.SessionState;

import javax.inject.Inject;
import java.util.List;

public class MetaTableGrpcImpl extends GrpcTableOperation<MetaTableRequest> {

    @Inject
    public MetaTableGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionMetaTable, BatchTableRequest.Operation::getMetaTable,
                MetaTableRequest::getResultId, MetaTableRequest::getSourceId);
    }

    @Override
    public Table create(final MetaTableRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        final Table parent = sourceTables.get(0).get();
        return parent.getMeta();
    }
}
