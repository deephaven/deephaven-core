//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.auth.codegen.impl;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.ServiceAuthWiring;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.AggregateAllRequest;
import io.deephaven.proto.backplane.grpc.AggregateRequest;
import io.deephaven.proto.backplane.grpc.AjRajTablesRequest;
import io.deephaven.proto.backplane.grpc.ApplyPreviewColumnsRequest;
import io.deephaven.proto.backplane.grpc.AsOfJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.ColumnStatisticsRequest;
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
import io.deephaven.proto.backplane.grpc.MetaTableRequest;
import io.deephaven.proto.backplane.grpc.MultiJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.NaturalJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.RunChartDownsampleRequest;
import io.deephaven.proto.backplane.grpc.SeekRowRequest;
import io.deephaven.proto.backplane.grpc.SelectDistinctRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.SliceRequest;
import io.deephaven.proto.backplane.grpc.SnapshotTableRequest;
import io.deephaven.proto.backplane.grpc.SnapshotWhenTableRequest;
import io.deephaven.proto.backplane.grpc.SortTableRequest;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import io.deephaven.proto.backplane.grpc.UngroupRequest;
import io.deephaven.proto.backplane.grpc.UnstructuredFilterTableRequest;
import io.deephaven.proto.backplane.grpc.UpdateByRequest;
import io.deephaven.proto.backplane.grpc.WhereInRequest;
import java.lang.Override;
import java.util.List;

/**
 * This interface provides type-safe authorization hooks for TableServiceGrpc.
 */
public interface TableServiceContextualAuthWiring {
    /**
     * Authorize a request to GetExportedTableCreationResponse.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke GetExportedTableCreationResponse
     */
    void checkPermissionGetExportedTableCreationResponse(AuthContext authContext, Ticket request,
            List<Table> sourceTables);

    /**
     * Authorize a request to FetchTable.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke FetchTable
     */
    void checkPermissionFetchTable(AuthContext authContext, FetchTableRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to ApplyPreviewColumns.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke ApplyPreviewColumns
     */
    void checkPermissionApplyPreviewColumns(AuthContext authContext,
            ApplyPreviewColumnsRequest request, List<Table> sourceTables);

    /**
     * Authorize a request to EmptyTable.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke EmptyTable
     */
    void checkPermissionEmptyTable(AuthContext authContext, EmptyTableRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to TimeTable.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke TimeTable
     */
    void checkPermissionTimeTable(AuthContext authContext, TimeTableRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to DropColumns.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke DropColumns
     */
    void checkPermissionDropColumns(AuthContext authContext, DropColumnsRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to Update.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke Update
     */
    void checkPermissionUpdate(AuthContext authContext, SelectOrUpdateRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to LazyUpdate.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke LazyUpdate
     */
    void checkPermissionLazyUpdate(AuthContext authContext, SelectOrUpdateRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to View.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke View
     */
    void checkPermissionView(AuthContext authContext, SelectOrUpdateRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to UpdateView.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke UpdateView
     */
    void checkPermissionUpdateView(AuthContext authContext, SelectOrUpdateRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to Select.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke Select
     */
    void checkPermissionSelect(AuthContext authContext, SelectOrUpdateRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to UpdateBy.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke UpdateBy
     */
    void checkPermissionUpdateBy(AuthContext authContext, UpdateByRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to SelectDistinct.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke SelectDistinct
     */
    void checkPermissionSelectDistinct(AuthContext authContext, SelectDistinctRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to Filter.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke Filter
     */
    void checkPermissionFilter(AuthContext authContext, FilterTableRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to UnstructuredFilter.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke UnstructuredFilter
     */
    void checkPermissionUnstructuredFilter(AuthContext authContext,
            UnstructuredFilterTableRequest request, List<Table> sourceTables);

    /**
     * Authorize a request to Sort.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke Sort
     */
    void checkPermissionSort(AuthContext authContext, SortTableRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to Head.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke Head
     */
    void checkPermissionHead(AuthContext authContext, HeadOrTailRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to Tail.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke Tail
     */
    void checkPermissionTail(AuthContext authContext, HeadOrTailRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to HeadBy.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke HeadBy
     */
    void checkPermissionHeadBy(AuthContext authContext, HeadOrTailByRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to TailBy.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke TailBy
     */
    void checkPermissionTailBy(AuthContext authContext, HeadOrTailByRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to Ungroup.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke Ungroup
     */
    void checkPermissionUngroup(AuthContext authContext, UngroupRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to MergeTables.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke MergeTables
     */
    void checkPermissionMergeTables(AuthContext authContext, MergeTablesRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to CrossJoinTables.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke CrossJoinTables
     */
    void checkPermissionCrossJoinTables(AuthContext authContext, CrossJoinTablesRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to NaturalJoinTables.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke NaturalJoinTables
     */
    void checkPermissionNaturalJoinTables(AuthContext authContext, NaturalJoinTablesRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to ExactJoinTables.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke ExactJoinTables
     */
    void checkPermissionExactJoinTables(AuthContext authContext, ExactJoinTablesRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to LeftJoinTables.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke LeftJoinTables
     */
    void checkPermissionLeftJoinTables(AuthContext authContext, LeftJoinTablesRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to AsOfJoinTables.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke AsOfJoinTables
     */
    void checkPermissionAsOfJoinTables(AuthContext authContext, AsOfJoinTablesRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to AjTables.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke AjTables
     */
    void checkPermissionAjTables(AuthContext authContext, AjRajTablesRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to RajTables.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke RajTables
     */
    void checkPermissionRajTables(AuthContext authContext, AjRajTablesRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to MultiJoinTables.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke MultiJoinTables
     */
    void checkPermissionMultiJoinTables(AuthContext authContext, MultiJoinTablesRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to RangeJoinTables.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke RangeJoinTables
     */
    void checkPermissionRangeJoinTables(AuthContext authContext, RangeJoinTablesRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to ComboAggregate.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke ComboAggregate
     */
    void checkPermissionComboAggregate(AuthContext authContext, ComboAggregateRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to AggregateAll.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke AggregateAll
     */
    void checkPermissionAggregateAll(AuthContext authContext, AggregateAllRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to Aggregate.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke Aggregate
     */
    void checkPermissionAggregate(AuthContext authContext, AggregateRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to Snapshot.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke Snapshot
     */
    void checkPermissionSnapshot(AuthContext authContext, SnapshotTableRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to SnapshotWhen.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke SnapshotWhen
     */
    void checkPermissionSnapshotWhen(AuthContext authContext, SnapshotWhenTableRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to Flatten.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke Flatten
     */
    void checkPermissionFlatten(AuthContext authContext, FlattenRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to RunChartDownsample.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke RunChartDownsample
     */
    void checkPermissionRunChartDownsample(AuthContext authContext, RunChartDownsampleRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to CreateInputTable.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke CreateInputTable
     */
    void checkPermissionCreateInputTable(AuthContext authContext, CreateInputTableRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to WhereIn.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke WhereIn
     */
    void checkPermissionWhereIn(AuthContext authContext, WhereInRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to ExportedTableUpdates.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke ExportedTableUpdates
     */
    void checkPermissionExportedTableUpdates(AuthContext authContext,
            ExportedTableUpdatesRequest request, List<Table> sourceTables);

    /**
     * Authorize a request to SeekRow.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke SeekRow
     */
    void checkPermissionSeekRow(AuthContext authContext, SeekRowRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to MetaTable.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke MetaTable
     */
    void checkPermissionMetaTable(AuthContext authContext, MetaTableRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to ComputeColumnStatistics.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke ComputeColumnStatistics
     */
    void checkPermissionComputeColumnStatistics(AuthContext authContext,
            ColumnStatisticsRequest request, List<Table> sourceTables);

    /**
     * Authorize a request to Slice.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke Slice
     */
    void checkPermissionSlice(AuthContext authContext, SliceRequest request,
            List<Table> sourceTables);

    /**
     * A default implementation that funnels all requests to invoke {@code checkPermission}.
     */
    abstract class DelegateAll implements TableServiceContextualAuthWiring {
        protected abstract void checkPermission(AuthContext authContext, List<Table> sourceTables);

        public void checkPermissionGetExportedTableCreationResponse(AuthContext authContext,
                Ticket request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionFetchTable(AuthContext authContext, FetchTableRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionApplyPreviewColumns(AuthContext authContext,
                ApplyPreviewColumnsRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionEmptyTable(AuthContext authContext, EmptyTableRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionTimeTable(AuthContext authContext, TimeTableRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionDropColumns(AuthContext authContext, DropColumnsRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionUpdate(AuthContext authContext, SelectOrUpdateRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionLazyUpdate(AuthContext authContext, SelectOrUpdateRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionView(AuthContext authContext, SelectOrUpdateRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionUpdateView(AuthContext authContext, SelectOrUpdateRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionSelect(AuthContext authContext, SelectOrUpdateRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionUpdateBy(AuthContext authContext, UpdateByRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionSelectDistinct(AuthContext authContext,
                SelectDistinctRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionFilter(AuthContext authContext, FilterTableRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionUnstructuredFilter(AuthContext authContext,
                UnstructuredFilterTableRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionSort(AuthContext authContext, SortTableRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionHead(AuthContext authContext, HeadOrTailRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionTail(AuthContext authContext, HeadOrTailRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionHeadBy(AuthContext authContext, HeadOrTailByRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionTailBy(AuthContext authContext, HeadOrTailByRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionUngroup(AuthContext authContext, UngroupRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionMergeTables(AuthContext authContext, MergeTablesRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionCrossJoinTables(AuthContext authContext,
                CrossJoinTablesRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionNaturalJoinTables(AuthContext authContext,
                NaturalJoinTablesRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionExactJoinTables(AuthContext authContext,
                ExactJoinTablesRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionLeftJoinTables(AuthContext authContext,
                LeftJoinTablesRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionAsOfJoinTables(AuthContext authContext,
                AsOfJoinTablesRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionAjTables(AuthContext authContext, AjRajTablesRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionRajTables(AuthContext authContext, AjRajTablesRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionMultiJoinTables(AuthContext authContext,
                MultiJoinTablesRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionRangeJoinTables(AuthContext authContext,
                RangeJoinTablesRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionComboAggregate(AuthContext authContext,
                ComboAggregateRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionAggregateAll(AuthContext authContext, AggregateAllRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionAggregate(AuthContext authContext, AggregateRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionSnapshot(AuthContext authContext, SnapshotTableRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionSnapshotWhen(AuthContext authContext,
                SnapshotWhenTableRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionFlatten(AuthContext authContext, FlattenRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionRunChartDownsample(AuthContext authContext,
                RunChartDownsampleRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionCreateInputTable(AuthContext authContext,
                CreateInputTableRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionWhereIn(AuthContext authContext, WhereInRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionExportedTableUpdates(AuthContext authContext,
                ExportedTableUpdatesRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionSeekRow(AuthContext authContext, SeekRowRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionMetaTable(AuthContext authContext, MetaTableRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionComputeColumnStatistics(AuthContext authContext,
                ColumnStatisticsRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionSlice(AuthContext authContext, SliceRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }
    }

    /**
     * A default implementation that allows all requests.
     */
    class AllowAll extends DelegateAll {
        @Override
        protected void checkPermission(AuthContext authContext, List<Table> sourceTables) {}
    }

    /**
     * A default implementation that denies all requests.
     */
    class DenyAll extends DelegateAll {
        @Override
        protected void checkPermission(AuthContext authContext, List<Table> sourceTables) {
            ServiceAuthWiring.operationNotAllowed();
        }
    }

    class TestUseOnly implements TableServiceContextualAuthWiring {
        public TableServiceContextualAuthWiring delegate;

        public void checkPermissionGetExportedTableCreationResponse(AuthContext authContext,
                Ticket request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionGetExportedTableCreationResponse(authContext, request, sourceTables);
            }
        }

        public void checkPermissionFetchTable(AuthContext authContext, FetchTableRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionFetchTable(authContext, request, sourceTables);
            }
        }

        public void checkPermissionApplyPreviewColumns(AuthContext authContext,
                ApplyPreviewColumnsRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionApplyPreviewColumns(authContext, request, sourceTables);
            }
        }

        public void checkPermissionEmptyTable(AuthContext authContext, EmptyTableRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionEmptyTable(authContext, request, sourceTables);
            }
        }

        public void checkPermissionTimeTable(AuthContext authContext, TimeTableRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionTimeTable(authContext, request, sourceTables);
            }
        }

        public void checkPermissionDropColumns(AuthContext authContext, DropColumnsRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionDropColumns(authContext, request, sourceTables);
            }
        }

        public void checkPermissionUpdate(AuthContext authContext, SelectOrUpdateRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionUpdate(authContext, request, sourceTables);
            }
        }

        public void checkPermissionLazyUpdate(AuthContext authContext, SelectOrUpdateRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionLazyUpdate(authContext, request, sourceTables);
            }
        }

        public void checkPermissionView(AuthContext authContext, SelectOrUpdateRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionView(authContext, request, sourceTables);
            }
        }

        public void checkPermissionUpdateView(AuthContext authContext, SelectOrUpdateRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionUpdateView(authContext, request, sourceTables);
            }
        }

        public void checkPermissionSelect(AuthContext authContext, SelectOrUpdateRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionSelect(authContext, request, sourceTables);
            }
        }

        public void checkPermissionUpdateBy(AuthContext authContext, UpdateByRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionUpdateBy(authContext, request, sourceTables);
            }
        }

        public void checkPermissionSelectDistinct(AuthContext authContext,
                SelectDistinctRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionSelectDistinct(authContext, request, sourceTables);
            }
        }

        public void checkPermissionFilter(AuthContext authContext, FilterTableRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionFilter(authContext, request, sourceTables);
            }
        }

        public void checkPermissionUnstructuredFilter(AuthContext authContext,
                UnstructuredFilterTableRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionUnstructuredFilter(authContext, request, sourceTables);
            }
        }

        public void checkPermissionSort(AuthContext authContext, SortTableRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionSort(authContext, request, sourceTables);
            }
        }

        public void checkPermissionHead(AuthContext authContext, HeadOrTailRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionHead(authContext, request, sourceTables);
            }
        }

        public void checkPermissionTail(AuthContext authContext, HeadOrTailRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionTail(authContext, request, sourceTables);
            }
        }

        public void checkPermissionHeadBy(AuthContext authContext, HeadOrTailByRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionHeadBy(authContext, request, sourceTables);
            }
        }

        public void checkPermissionTailBy(AuthContext authContext, HeadOrTailByRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionTailBy(authContext, request, sourceTables);
            }
        }

        public void checkPermissionUngroup(AuthContext authContext, UngroupRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionUngroup(authContext, request, sourceTables);
            }
        }

        public void checkPermissionMergeTables(AuthContext authContext, MergeTablesRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionMergeTables(authContext, request, sourceTables);
            }
        }

        public void checkPermissionCrossJoinTables(AuthContext authContext,
                CrossJoinTablesRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionCrossJoinTables(authContext, request, sourceTables);
            }
        }

        public void checkPermissionNaturalJoinTables(AuthContext authContext,
                NaturalJoinTablesRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionNaturalJoinTables(authContext, request, sourceTables);
            }
        }

        public void checkPermissionExactJoinTables(AuthContext authContext,
                ExactJoinTablesRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionExactJoinTables(authContext, request, sourceTables);
            }
        }

        public void checkPermissionLeftJoinTables(AuthContext authContext,
                LeftJoinTablesRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionLeftJoinTables(authContext, request, sourceTables);
            }
        }

        public void checkPermissionAsOfJoinTables(AuthContext authContext,
                AsOfJoinTablesRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionAsOfJoinTables(authContext, request, sourceTables);
            }
        }

        public void checkPermissionAjTables(AuthContext authContext, AjRajTablesRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionAjTables(authContext, request, sourceTables);
            }
        }

        public void checkPermissionRajTables(AuthContext authContext, AjRajTablesRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionRajTables(authContext, request, sourceTables);
            }
        }

        public void checkPermissionMultiJoinTables(AuthContext authContext,
                MultiJoinTablesRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionMultiJoinTables(authContext, request, sourceTables);
            }
        }

        public void checkPermissionRangeJoinTables(AuthContext authContext,
                RangeJoinTablesRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionRangeJoinTables(authContext, request, sourceTables);
            }
        }

        public void checkPermissionComboAggregate(AuthContext authContext,
                ComboAggregateRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionComboAggregate(authContext, request, sourceTables);
            }
        }

        public void checkPermissionAggregateAll(AuthContext authContext, AggregateAllRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionAggregateAll(authContext, request, sourceTables);
            }
        }

        public void checkPermissionAggregate(AuthContext authContext, AggregateRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionAggregate(authContext, request, sourceTables);
            }
        }

        public void checkPermissionSnapshot(AuthContext authContext, SnapshotTableRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionSnapshot(authContext, request, sourceTables);
            }
        }

        public void checkPermissionSnapshotWhen(AuthContext authContext,
                SnapshotWhenTableRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionSnapshotWhen(authContext, request, sourceTables);
            }
        }

        public void checkPermissionFlatten(AuthContext authContext, FlattenRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionFlatten(authContext, request, sourceTables);
            }
        }

        public void checkPermissionRunChartDownsample(AuthContext authContext,
                RunChartDownsampleRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionRunChartDownsample(authContext, request, sourceTables);
            }
        }

        public void checkPermissionCreateInputTable(AuthContext authContext,
                CreateInputTableRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionCreateInputTable(authContext, request, sourceTables);
            }
        }

        public void checkPermissionWhereIn(AuthContext authContext, WhereInRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionWhereIn(authContext, request, sourceTables);
            }
        }

        public void checkPermissionExportedTableUpdates(AuthContext authContext,
                ExportedTableUpdatesRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionExportedTableUpdates(authContext, request, sourceTables);
            }
        }

        public void checkPermissionSeekRow(AuthContext authContext, SeekRowRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionSeekRow(authContext, request, sourceTables);
            }
        }

        public void checkPermissionMetaTable(AuthContext authContext, MetaTableRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionMetaTable(authContext, request, sourceTables);
            }
        }

        public void checkPermissionComputeColumnStatistics(AuthContext authContext,
                ColumnStatisticsRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionComputeColumnStatistics(authContext, request, sourceTables);
            }
        }

        public void checkPermissionSlice(AuthContext authContext, SliceRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionSlice(authContext, request, sourceTables);
            }
        }
    }
}
