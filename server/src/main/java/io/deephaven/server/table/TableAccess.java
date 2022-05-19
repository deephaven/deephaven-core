package io.deephaven.server.table;

import io.deephaven.proto.backplane.grpc.ApplyPreviewColumnsRequest;
import io.deephaven.proto.backplane.grpc.AsOfJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
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
import io.deephaven.server.session.SessionState;

public interface TableAccess {

    // TODO: better as QST TableSpec instead of grpc?

    SessionState emptyTable(EmptyTableRequest request);

    SessionState timeTable(TimeTableRequest request);

    SessionState mergeTables(MergeTablesRequest request);

    SessionState selectDistinct(SelectDistinctRequest request);

    SessionState update(SelectOrUpdateRequest request);

    SessionState lazyUpdate(SelectOrUpdateRequest request);

    SessionState view(SelectOrUpdateRequest request);

    SessionState updateView(SelectOrUpdateRequest request);

    SessionState select(SelectOrUpdateRequest request);

    SessionState headBy(HeadOrTailByRequest request);

    SessionState tailBy(HeadOrTailByRequest request);

    SessionState head(HeadOrTailRequest request);

    SessionState tail(HeadOrTailRequest request);

    SessionState ungroup(UngroupRequest request);

    SessionState comboAggregate(ComboAggregateRequest request);

    SessionState snapshot(SnapshotTableRequest request);

    SessionState dropColumns(DropColumnsRequest request);

    SessionState filter(FilterTableRequest request);

    SessionState unstructuredFilter(UnstructuredFilterTableRequest request);

    SessionState sort(SortTableRequest request);

    SessionState flatten(FlattenRequest request);

    SessionState crossJoinTables(CrossJoinTablesRequest request);

    SessionState naturalJoinTables(NaturalJoinTablesRequest request);

    SessionState exactJoinTables(ExactJoinTablesRequest request);

    SessionState leftJoinTables(LeftJoinTablesRequest request);

    SessionState asOfJoinTables(AsOfJoinTablesRequest request);

    SessionState runChartDownsample(RunChartDownsampleRequest request);

    SessionState fetchTable(FetchTableRequest request);

    SessionState applyPreviewColumns(ApplyPreviewColumnsRequest request);

    SessionState createInputTable(CreateInputTableRequest request);

    SessionState batch(BatchTableRequest request);

    SessionState exportedTableUpdates(ExportedTableUpdatesRequest request);

    SessionState getExportedTableCreationResponse(Ticket request);
}
