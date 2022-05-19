package io.deephaven.server.table;

import io.deephaven.proto.backplane.grpc.ApplyPreviewColumnsRequest;
import io.deephaven.proto.backplane.grpc.AsOfJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest;
import io.deephaven.proto.backplane.grpc.CrossJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.DropColumnsRequest;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.ExactJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdatesRequest;
import io.deephaven.proto.backplane.grpc.FetchTableRequest;
import io.deephaven.proto.backplane.grpc.FilterTableRequest;
import io.deephaven.proto.backplane.grpc.FlattenRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailByRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailRequest;
import io.deephaven.proto.backplane.grpc.LeftJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.MergeTablesRequest;
import io.deephaven.proto.backplane.grpc.NaturalJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.RunChartDownsampleRequest;
import io.deephaven.proto.backplane.grpc.SelectDistinctRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.SnapshotTableRequest;
import io.deephaven.proto.backplane.grpc.SortTableRequest;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import io.deephaven.proto.backplane.grpc.UngroupRequest;
import io.deephaven.proto.backplane.grpc.UnstructuredFilterTableRequest;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;

import java.util.Objects;

public class TableAccessDefaultImpl extends TableAccessBatchDelegator {
    private final SessionService sessionService;

    public TableAccessDefaultImpl(SessionService sessionService) {
        this.sessionService = Objects.requireNonNull(sessionService);
    }

    private SessionState accept() {
        return sessionService.getCurrentSession();
    }

    @Override
    public SessionState emptyTable(EmptyTableRequest request) {
        return accept();
    }


    @Override
    public SessionState timeTable(TimeTableRequest request) {
        return accept();
    }

    @Override
    public SessionState mergeTables(MergeTablesRequest request) {
        return accept();
    }

    @Override
    public SessionState selectDistinct(SelectDistinctRequest request) {
        return accept();
    }

    @Override
    public SessionState update(SelectOrUpdateRequest request) {
        return accept();
    }

    @Override
    public SessionState lazyUpdate(SelectOrUpdateRequest request) {
        return accept();
    }

    @Override
    public SessionState view(SelectOrUpdateRequest request) {
        return accept();
    }

    @Override
    public SessionState updateView(SelectOrUpdateRequest request) {
        return accept();
    }

    @Override
    public SessionState select(SelectOrUpdateRequest request) {
        return accept();
    }

    @Override
    public SessionState headBy(HeadOrTailByRequest request) {
        return accept();
    }

    @Override
    public SessionState tailBy(HeadOrTailByRequest request) {
        return accept();
    }

    @Override
    public SessionState head(HeadOrTailRequest request) {
        return accept();
    }

    @Override
    public SessionState tail(HeadOrTailRequest request) {
        return accept();
    }

    @Override
    public SessionState ungroup(UngroupRequest request) {
        return accept();
    }

    @Override
    public SessionState comboAggregate(ComboAggregateRequest request) {
        return accept();
    }

    @Override
    public SessionState snapshot(SnapshotTableRequest request) {
        return accept();
    }

    @Override
    public SessionState dropColumns(DropColumnsRequest request) {
        return accept();
    }

    @Override
    public SessionState filter(FilterTableRequest request) {
        return accept();
    }

    @Override
    public SessionState unstructuredFilter(UnstructuredFilterTableRequest request) {
        return accept();
    }

    @Override
    public SessionState sort(SortTableRequest request) {
        return accept();
    }

    @Override
    public SessionState flatten(FlattenRequest request) {
        return accept();
    }

    @Override
    public SessionState crossJoinTables(CrossJoinTablesRequest request) {
        return accept();
    }

    @Override
    public SessionState naturalJoinTables(NaturalJoinTablesRequest request) {
        return accept();
    }

    @Override
    public SessionState exactJoinTables(ExactJoinTablesRequest request) {
        return accept();
    }

    @Override
    public SessionState leftJoinTables(LeftJoinTablesRequest request) {
        return accept();
    }

    @Override
    public SessionState asOfJoinTables(AsOfJoinTablesRequest request) {
        return accept();
    }

    @Override
    public SessionState runChartDownsample(RunChartDownsampleRequest request) {
        return accept();
    }

    @Override
    public SessionState fetchTable(FetchTableRequest request) {
        return accept();
    }

    @Override
    public SessionState applyPreviewColumns(ApplyPreviewColumnsRequest request) {
        return accept();
    }

    @Override
    public SessionState createInputTable(CreateInputTableRequest request) {
        return accept();
    }

    @Override
    public SessionState exportedTableUpdates(ExportedTableUpdatesRequest request) {
        return accept();
    }

    @Override
    public SessionState getExportedTableCreationResponse(Ticket request) {
        return accept();
    }
}
