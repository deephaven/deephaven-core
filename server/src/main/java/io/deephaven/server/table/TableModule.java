/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table;

import dagger.Binds;
import dagger.MapKey;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoMap;
import dagger.multibindings.IntoSet;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.table.ops.AggregateAllGrpcImpl;
import io.deephaven.server.table.ops.AggregateGrpcImpl;
import io.deephaven.server.table.ops.ApplyPreviewColumnsGrpcImpl;
import io.deephaven.server.table.ops.ComboAggregateGrpcImpl;
import io.deephaven.server.table.ops.CreateInputTableGrpcImpl;
import io.deephaven.server.table.ops.DropColumnsGrpcImpl;
import io.deephaven.server.table.ops.EmptyTableGrpcImpl;
import io.deephaven.server.table.ops.FetchTableGrpcImpl;
import io.deephaven.server.table.ops.FilterTableGrpcImpl;
import io.deephaven.server.table.ops.FlattenTableGrpcImpl;
import io.deephaven.server.table.ops.GrpcTableOperation;
import io.deephaven.server.table.ops.HeadOrTailByGrpcImpl;
import io.deephaven.server.table.ops.HeadOrTailGrpcImpl;
import io.deephaven.server.table.ops.JoinTablesGrpcImpl;
import io.deephaven.server.table.ops.MergeTablesGrpcImpl;
import io.deephaven.server.table.ops.MetaTableGrpcImpl;
import io.deephaven.server.table.ops.RangeJoinGrpcImpl;
import io.deephaven.server.table.ops.RunChartDownsampleGrpcImpl;
import io.deephaven.server.table.ops.SelectDistinctGrpcImpl;
import io.deephaven.server.table.ops.SnapshotTableGrpcImpl;
import io.deephaven.server.table.ops.SnapshotWhenTableGrpcImpl;
import io.deephaven.server.table.ops.SortTableGrpcImpl;
import io.deephaven.server.table.ops.TableServiceGrpcImpl;
import io.deephaven.server.table.ops.TimeTableGrpcImpl;
import io.deephaven.server.table.ops.UngroupGrpcImpl;
import io.deephaven.server.table.ops.UnstructuredFilterTableGrpcImpl;
import io.deephaven.server.table.ops.UpdateByGrpcImpl;
import io.deephaven.server.table.ops.UpdateOrSelectGrpcImpl;
import io.deephaven.server.table.ops.WhereInGrpcImpl;
import io.grpc.BindableService;

@MapKey
@interface BatchOpCode {
    BatchTableRequest.Operation.OpCase value();
}


@Module
public interface TableModule {
    @Provides
    static TableServiceContextualAuthWiring provideAuthWiring(AuthorizationProvider authProvider) {
        return authProvider.getTableServiceContextualAuthWiring();
    }

    @Binds
    @IntoSet
    BindableService bindTableServiceGrpcImpl(TableServiceGrpcImpl tableService);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.EMPTY_TABLE)
    GrpcTableOperation<?> bindOperationEmptyTable(EmptyTableGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.TIME_TABLE)
    GrpcTableOperation<?> bindOperationTimeTable(TimeTableGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.MERGE)
    GrpcTableOperation<?> bindOperationMergeTables(MergeTablesGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.SELECT_DISTINCT)
    GrpcTableOperation<?> bindOperationSelectDistinct(SelectDistinctGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.UPDATE)
    GrpcTableOperation<?> bindOperationUpdate(UpdateOrSelectGrpcImpl.UpdateGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.LAZY_UPDATE)
    GrpcTableOperation<?> bindOperationLazyUpdate(UpdateOrSelectGrpcImpl.LazyUpdateGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.VIEW)
    GrpcTableOperation<?> bindOperationView(UpdateOrSelectGrpcImpl.ViewGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.UPDATE_VIEW)
    GrpcTableOperation<?> bindOperationUpdateView(UpdateOrSelectGrpcImpl.UpdateViewGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.SELECT)
    GrpcTableOperation<?> bindOperationSelect(UpdateOrSelectGrpcImpl.SelectGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.HEAD_BY)
    GrpcTableOperation<?> bindOperationHeadBy(HeadOrTailByGrpcImpl.HeadByGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.TAIL_BY)
    GrpcTableOperation<?> bindOperationTailBy(HeadOrTailByGrpcImpl.TailByGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.HEAD)
    GrpcTableOperation<?> bindOperationHead(HeadOrTailGrpcImpl.HeadGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.TAIL)
    GrpcTableOperation<?> bindOperationTail(HeadOrTailGrpcImpl.TailGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.UNGROUP)
    GrpcTableOperation<?> bindOperationUngroup(UngroupGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.COMBO_AGGREGATE)
    GrpcTableOperation<?> bindOperationComboAgg(ComboAggregateGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.AGGREGATE_ALL)
    GrpcTableOperation<?> bindOperationAggregateAll(AggregateAllGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.AGGREGATE)
    GrpcTableOperation<?> bindOperationAggregate(AggregateGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.CROSS_JOIN)
    GrpcTableOperation<?> bindOperationCrossJoin(JoinTablesGrpcImpl.CrossJoinTablesGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.EXACT_JOIN)
    GrpcTableOperation<?> bindOperationExactJoin(JoinTablesGrpcImpl.ExactJoinTablesGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.LEFT_JOIN)
    GrpcTableOperation<?> bindOperationLeftJoin(JoinTablesGrpcImpl.LeftJoinTablesGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.NATURAL_JOIN)
    GrpcTableOperation<?> bindOperationNaturalJoin(JoinTablesGrpcImpl.NaturalJoinTablesGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.FILTER)
    GrpcTableOperation<?> bindOperationFilterTable(FilterTableGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.UNSTRUCTURED_FILTER)
    GrpcTableOperation<?> bindOperationUnstructuredFilterTable(UnstructuredFilterTableGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.SNAPSHOT)
    GrpcTableOperation<?> bindOperationSnapshotTable(SnapshotTableGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.SNAPSHOT_WHEN)
    GrpcTableOperation<?> bindOperationSnapshotWhenTable(SnapshotWhenTableGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.SORT)
    GrpcTableOperation<?> bindOperationSortTable(SortTableGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.DROP_COLUMNS)
    GrpcTableOperation<?> bindOperationDropColumns(DropColumnsGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.FLATTEN)
    GrpcTableOperation<?> bindOperationFlatten(FlattenTableGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.AS_OF_JOIN)
    GrpcTableOperation<?> bindOperationAsOfJoin(JoinTablesGrpcImpl.AsOfJoinTablesGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.RANGE_JOIN)
    GrpcTableOperation<?> bindOperationRangeJoin(RangeJoinGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.RUN_CHART_DOWNSAMPLE)
    GrpcTableOperation<?> bindOperationRunChartDownsample(RunChartDownsampleGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.FETCH_TABLE)
    GrpcTableOperation<?> bindFetchTable(FetchTableGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.APPLY_PREVIEW_COLUMNS)
    GrpcTableOperation<?> bindApplyPreviewColumns(ApplyPreviewColumnsGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.CREATE_INPUT_TABLE)
    GrpcTableOperation<?> bindCreateInputTable(CreateInputTableGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.UPDATE_BY)
    GrpcTableOperation<?> bindUpdateBy(UpdateByGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.WHERE_IN)
    GrpcTableOperation<?> bindWhereIn(WhereInGrpcImpl op);

    @Binds
    @IntoMap
    @BatchOpCode(BatchTableRequest.Operation.OpCase.META_TABLE)
    GrpcTableOperation<?> bindMeta(MetaTableGrpcImpl op);

}
