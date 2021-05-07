package io.deephaven.grpc_api.table;

import dagger.Binds;
import dagger.MapKey;
import dagger.Module;
import dagger.multibindings.IntoMap;
import dagger.multibindings.IntoSet;
import io.deephaven.grpc_api.table.ops.ComboAggregateGrpcImpl;
import io.deephaven.grpc_api.table.ops.DropColumnsGrpcImpl;
import io.deephaven.grpc_api.table.ops.EmptyTableGrpcImpl;
import io.deephaven.grpc_api.table.ops.FilterTableGrpcImpl;
import io.deephaven.grpc_api.table.ops.FlattenTableGrpcImpl;
import io.deephaven.grpc_api.table.ops.GrpcTableOperation;
import io.deephaven.grpc_api.table.ops.HeadOrTailByGrpcImpl;
import io.deephaven.grpc_api.table.ops.HeadOrTailGrpcImpl;
import io.deephaven.grpc_api.table.ops.JoinTablesGrpcImpl;
import io.deephaven.grpc_api.table.ops.MergeTablesGrpcImpl;
import io.deephaven.grpc_api.table.ops.SelectDistinctGrpcImpl;
import io.deephaven.grpc_api.table.ops.SnapshotTableGrpcImpl;
import io.deephaven.grpc_api.table.ops.SortTableGrpcImpl;
import io.deephaven.grpc_api.table.ops.TimeTableGrpcImpl;
import io.deephaven.grpc_api.table.ops.UngroupGrpcImpl;
import io.deephaven.grpc_api.table.ops.UnstructuredFilterTableGrpcImpl;
import io.deephaven.grpc_api.table.ops.UpdateOrSelectGrpcImpl;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.grpc.BindableService;

@MapKey
@interface BatchOpCode {
    BatchTableRequest.Operation.OpCase value();
}

@Module
public interface TableModule {
    @Binds @IntoSet
    BindableService bindTableServiceGrpcImpl(TableServiceGrpcImpl tableService);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.EMPTYTABLE)
    GrpcTableOperation<?> bindOperationEmptyTable(EmptyTableGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.TIMETABLE)
    GrpcTableOperation<?> bindOperationTimeTable(TimeTableGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.MERGE)
    GrpcTableOperation<?> bindOperationMergeTables(MergeTablesGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.SELECTDISTINCT)
    GrpcTableOperation<?> bindOperationSelectDistinct(SelectDistinctGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.UPDATE)
    GrpcTableOperation<?> bindOperationUpdate(UpdateOrSelectGrpcImpl.UpdateGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.LAZYUPDATE)
    GrpcTableOperation<?> bindOperationLazyUpdate(UpdateOrSelectGrpcImpl.LazyUpdateGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.VIEW)
    GrpcTableOperation<?> bindOperationView(UpdateOrSelectGrpcImpl.ViewGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.UPDATEVIEW)
    GrpcTableOperation<?> bindOperationUpdateView(UpdateOrSelectGrpcImpl.UpdateViewGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.SELECT)
    GrpcTableOperation<?> bindOperationSelect(UpdateOrSelectGrpcImpl.SelectGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.HEADBY)
    GrpcTableOperation<?> bindOperationHeadBy(HeadOrTailByGrpcImpl.HeadByGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.TAILBY)
    GrpcTableOperation<?> bindOperationTailBy(HeadOrTailByGrpcImpl.TailByGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.HEAD)
    GrpcTableOperation<?> bindOperationHead(HeadOrTailGrpcImpl.HeadGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.TAIL)
    GrpcTableOperation<?> bindOperationTail(HeadOrTailGrpcImpl.TailGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.UNGROUP)
    GrpcTableOperation<?> bindOperationUngroup(UngroupGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.COMBOAGGREGATE)
    GrpcTableOperation<?> bindOperationComboAgg(ComboAggregateGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.JOIN)
    GrpcTableOperation<?> bindOperationJoinTables(JoinTablesGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.FILTER)
    GrpcTableOperation<?> bindOperationFilterTable(FilterTableGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.UNSTRUCTUREDFILTER)
    GrpcTableOperation<?> bindOperationUnstructuredFilterTable(UnstructuredFilterTableGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.SNAPSHOT)
    GrpcTableOperation<?> bindOperationSnapshotTable(SnapshotTableGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.SORT)
    GrpcTableOperation<?> bindOperationSortTable(SortTableGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.DROPCOLUMNS)
    GrpcTableOperation<?> bindOperationDropColumns(DropColumnsGrpcImpl op);

    @Binds @IntoMap @BatchOpCode(BatchTableRequest.Operation.OpCase.FLATTEN)
    GrpcTableOperation<?> bindOperationFlatten(FlattenTableGrpcImpl op);
}
