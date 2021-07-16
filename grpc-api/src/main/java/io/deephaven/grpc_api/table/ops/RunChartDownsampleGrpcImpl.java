package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import io.deephaven.clientsupport.plotdownsampling.RunChartDownsample;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.RunChartDownsampleRequest;

import javax.inject.Inject;
import java.util.List;

public class RunChartDownsampleGrpcImpl extends GrpcTableOperation<RunChartDownsampleRequest> {
    @Inject
    protected RunChartDownsampleGrpcImpl() {
        super(BatchTableRequest.Operation::getRunChartDownsample, RunChartDownsampleRequest::getResultId, RunChartDownsampleRequest::getSourceId);
    }

    @Override
    public Table create(RunChartDownsampleRequest request, List<SessionState.ExportObject<Table>> sourceTables) {
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
                request.getYColumnNamesList().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY)
        ));
    }
}
