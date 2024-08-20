//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.clientsupport.plotdownsampling.RunChartDownsample;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.RunChartDownsampleRequest;
import io.deephaven.server.session.SessionState;

import javax.inject.Inject;
import java.util.List;

public class RunChartDownsampleGrpcImpl extends GrpcTableOperation<RunChartDownsampleRequest> {
    @Inject
    protected RunChartDownsampleGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionRunChartDownsample, BatchTableRequest.Operation::getRunChartDownsample,
                RunChartDownsampleRequest::getResultId, RunChartDownsampleRequest::getSourceId);
    }

    @Override
    public Table create(final RunChartDownsampleRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);

        final Table parent = sourceTables.get(0).get();
        long[] zoomRange;
        if (request.hasZoomRange()) {
            zoomRange = new long[] {
                    request.getZoomRange().getMinDateNanos(),
                    request.getZoomRange().getMaxDateNanos()
            };
        } else {
            zoomRange = null;
        }
        return parent.apply(new RunChartDownsample(
                request.getPixelCount(),
                zoomRange,
                request.getXColumnName(),
                request.getYColumnNamesList().toArray(String[]::new)));
    }
}
