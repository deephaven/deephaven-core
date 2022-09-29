/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import io.deephaven.auth.AuthContext;
import io.deephaven.base.verify.Assert;
import io.deephaven.clientsupport.plotdownsampling.RunChartDownsample;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.RunChartDownsampleRequest;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.table.TableServicePrivilege;

import javax.inject.Inject;
import java.util.List;

public class RunChartDownsampleGrpcImpl extends GrpcTableOperation<RunChartDownsampleRequest> {
    @Inject
    protected RunChartDownsampleGrpcImpl() {
        super(BatchTableRequest.Operation::getRunChartDownsample, RunChartDownsampleRequest::getResultId,
                RunChartDownsampleRequest::getSourceId);
    }

    @Override
    public Table create(final AuthContext authContext,
            final RunChartDownsampleRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        authContext.requirePrivilege(TableServicePrivilege.CAN_RUN_CHART_DOWNSAMPLE);
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
                request.getYColumnNamesList().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY)));
    }
}
