/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.table;

import com.google.rpc.Code;
import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.barrage.util.BarrageSchemaUtil;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.session.TicketRouter;
import io.deephaven.grpc_api.table.ops.GrpcTableOperation;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.AsOfJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest;
import io.deephaven.proto.backplane.grpc.CrossJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.DropColumnsRequest;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.ExactJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateMessage;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdatesRequest;
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
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import io.deephaven.proto.backplane.grpc.UngroupRequest;
import io.deephaven.proto.backplane.grpc.UnstructuredFilterTableRequest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.deephaven.grpc_api.util.GrpcUtil.safelyExecute;
import static io.deephaven.grpc_api.util.GrpcUtil.safelyExecuteLocked;

public class TableServiceGrpcImpl extends TableServiceGrpc.TableServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(TableServiceGrpcImpl.class);

    private final TicketRouter ticketRouter;
    private final SessionService sessionService;
    private final Map<BatchTableRequest.Operation.OpCase, GrpcTableOperation<?>> operationMap;

    @Inject
    public TableServiceGrpcImpl(final TicketRouter ticketRouter,
        final SessionService sessionService,
        final Map<BatchTableRequest.Operation.OpCase, GrpcTableOperation<?>> operationMap) {
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
        this.operationMap = operationMap;
    }

    private <T> GrpcTableOperation<T> getOp(final BatchTableRequest.Operation.OpCase op) {
        // noinspection unchecked
        final GrpcTableOperation<T> operation = (GrpcTableOperation<T>) operationMap.get(op);
        if (operation == null) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                "BatchTableRequest.Operation.OpCode is unset, incompatible, or not yet supported. (found: "
                    + op + ")");
        }
        return operation;
    }

    @Override
    public void emptyTable(final EmptyTableRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.EMPTY_TABLE, request,
            responseObserver);
    }

    @Override
    public void timeTable(final TimeTableRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.TIME_TABLE, request,
            responseObserver);
    }

    @Override
    public void mergeTables(final MergeTablesRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.MERGE, request,
            responseObserver);
    }

    @Override
    public void selectDistinct(final SelectDistinctRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.SELECT_DISTINCT, request,
            responseObserver);
    }

    @Override
    public void update(final SelectOrUpdateRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.UPDATE, request,
            responseObserver);
    }

    @Override
    public void lazyUpdate(final SelectOrUpdateRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.LAZY_UPDATE, request,
            responseObserver);
    }

    @Override
    public void view(final SelectOrUpdateRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.VIEW, request, responseObserver);
    }

    @Override
    public void updateView(final SelectOrUpdateRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.UPDATE_VIEW, request,
            responseObserver);
    }

    @Override
    public void select(final SelectOrUpdateRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.SELECT, request,
            responseObserver);
    }

    @Override
    public void headBy(final HeadOrTailByRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.HEAD_BY, request,
            responseObserver);
    }

    @Override
    public void tailBy(final HeadOrTailByRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.TAIL_BY, request,
            responseObserver);
    }

    @Override
    public void head(final HeadOrTailRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.HEAD, request, responseObserver);
    }

    @Override
    public void tail(final HeadOrTailRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.TAIL, request, responseObserver);
    }

    @Override
    public void ungroup(final UngroupRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.UNGROUP, request,
            responseObserver);
    }

    @Override
    public void comboAggregate(final ComboAggregateRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.COMBO_AGGREGATE, request,
            responseObserver);
    }

    @Override
    public void snapshot(final SnapshotTableRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.SNAPSHOT, request,
            responseObserver);
    }

    @Override
    public void dropColumns(final DropColumnsRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.DROP_COLUMNS, request,
            responseObserver);
    }

    @Override
    public void filter(final FilterTableRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.FILTER, request,
            responseObserver);
    }

    @Override
    public void unstructuredFilter(final UnstructuredFilterTableRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.UNSTRUCTURED_FILTER, request,
            responseObserver);
    }

    @Override
    public void sort(final SortTableRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.SORT, request, responseObserver);
    }

    @Override
    public void flatten(final FlattenRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.FLATTEN, request,
            responseObserver);
    }

    @Override
    public void crossJoinTables(final CrossJoinTablesRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.CROSS_JOIN, request,
            responseObserver);
    }

    @Override
    public void naturalJoinTables(final NaturalJoinTablesRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.NATURAL_JOIN, request,
            responseObserver);
    }

    @Override
    public void exactJoinTables(final ExactJoinTablesRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.EXACT_JOIN, request,
            responseObserver);
    }

    @Override
    public void leftJoinTables(LeftJoinTablesRequest request,
        StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.LEFT_JOIN, request,
            responseObserver);
    }

    @Override
    public void asOfJoinTables(AsOfJoinTablesRequest request,
        StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.AS_OF_JOIN, request,
            responseObserver);
    }

    @Override
    public void runChartDownsample(RunChartDownsampleRequest request,
        StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.RUN_CHART_DOWNSAMPLE, request,
            responseObserver);
    }

    @Override
    public void batch(final BatchTableRequest request,
        final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            // step 1: initialize exports
            final List<BatchExportBuilder> exportBuilders = request.getOpsList().stream()
                .map(op -> new BatchExportBuilder(session, op))
                .collect(Collectors.toList());

            // step 2: resolve dependencies
            final Function<TableReference, SessionState.ExportObject<Table>> resolver = ref -> {
                // operations are allowed to return null for optional dependencies
                if (ref == null) {
                    return null;
                }

                switch (ref.getRefCase()) {
                    case TICKET:
                        return ticketRouter.resolve(session, ref.getTicket());
                    case BATCH_OFFSET:
                        final int offset = ref.getBatchOffset();
                        if (offset < 0 || offset >= exportBuilders.size()) {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                "invalid table reference: " + ref);
                        }
                        return exportBuilders.get(offset).exportBuilder.getExport();
                    default:
                        throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "invalid table reference: " + ref);
                }
            };
            exportBuilders.forEach(export -> export.resolveDependencies(resolver));

            // step 3: check for cyclical dependencies; this is our only opportunity to check
            // non-export cycles
            // TODO: check for cycles

            // step 4: submit the batched operations
            final AtomicInteger remaining = new AtomicInteger(exportBuilders.size());

            for (int i = 0; i < exportBuilders.size(); ++i) {
                final BatchExportBuilder exportBuilder = exportBuilders.get(i);
                final int exportId = exportBuilder.exportBuilder.getExportId();

                final TableReference resultId;
                if (exportId == SessionState.NON_EXPORT_ID) {
                    resultId = TableReference.newBuilder().setBatchOffset(i).build();
                } else {
                    resultId = TableReference.newBuilder()
                        .setTicket(ExportTicketHelper.exportIdToTicket(exportId)).build();
                }

                exportBuilder.exportBuilder.onError((result, errorContext, dependentId) -> {
                    safelyExecuteLocked(responseObserver,
                        () -> responseObserver.onNext(ExportedTableCreationResponse.newBuilder()
                            .setResultId(resultId)
                            .setSuccess(false)
                            .setErrorInfo(errorContext + " dependency: " + dependentId)
                            .build()));

                    if (remaining.decrementAndGet() == 0) {
                        safelyExecuteLocked(responseObserver, responseObserver::onCompleted);
                    }
                }).submit(() -> {
                    final Table table = exportBuilder.doExport();

                    safelyExecuteLocked(responseObserver,
                        () -> responseObserver.onNext(buildTableCreationResponse(resultId, table)));
                    if (remaining.decrementAndGet() == 0) {
                        safelyExecuteLocked(responseObserver, responseObserver::onCompleted);
                    }

                    return table;
                });
            }
        });
    }

    @Override
    public void exportedTableUpdates(final ExportedTableUpdatesRequest request,
        final StreamObserver<ExportedTableUpdateMessage> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();
            final ExportedTableUpdateListener listener =
                new ExportedTableUpdateListener(session, responseObserver);
            session.addExportListener(listener);
            ((ServerCallStreamObserver<ExportedTableUpdateMessage>) responseObserver)
                .setOnCancelHandler(() -> {
                    session.removeExportListener(listener);
                });
        });
    }

    public static ExportedTableCreationResponse buildTableCreationResponse(
        final TableReference tableRef, final Table table) {
        return ExportedTableCreationResponse.newBuilder()
            .setSuccess(true)
            .setResultId(tableRef)
            .setIsStatic(!table.isLive())
            .setSize(table.size())
            .setSchemaHeader(BarrageSchemaUtil.schemaBytesFromTable(table))
            .build();
    }

    /**
     * This helper is a wrapper that enables one-shot RPCs to utilize the same code paths that a
     * batch RPC utilizes.
     * 
     * @param op the protobuf op-code for the batch operation request
     * @param request the protobuf that is mapped to this op-code
     * @param responseObserver the observer that needs to know the result of this rpc
     * @param <T> the protobuf type that configures the behavior of the operation
     */
    private <T> void oneShotOperationWrapper(final BatchTableRequest.Operation.OpCase op,
        final T request, final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();
            final GrpcTableOperation<T> operation = getOp(op);
            operation.validateRequest(request);

            final Ticket resultId = operation.getResultTicket(request);
            final TableReference resultRef =
                TableReference.newBuilder().setTicket(resultId).build();

            final List<SessionState.ExportObject<Table>> dependencies =
                operation.getTableReferences(request).stream()
                    .map(TableReference::getTicket)
                    .map((ticket) -> ticketRouter.<Table>resolve(session, ticket))
                    .collect(Collectors.toList());

            session.newExport(resultId)
                .require(dependencies)
                .onError(responseObserver::onError)
                .submit(() -> {
                    final Table result = operation.create(request, dependencies);
                    safelyExecute(() -> {
                        responseObserver.onNext(buildTableCreationResponse(resultRef, result));
                        responseObserver.onCompleted();
                    });
                    return result;
                });
        });
    }

    private class BatchExportBuilder {
        final GrpcTableOperation<Object> operation;
        final Object request;
        final SessionState.ExportBuilder<Table> exportBuilder;

        List<SessionState.ExportObject<Table>> dependencies;

        BatchExportBuilder(final SessionState session, final BatchTableRequest.Operation op) {
            operation = getOp(op.getOpCase()); // get operation from op code
            request = operation.getRequestFromOperation(op);
            final Ticket resultId = operation.getResultTicket(request);
            exportBuilder = resultId.getTicket().size() == 0 ? session.nonExport()
                : session.newExport(resultId);
        }

        void resolveDependencies(
            final Function<TableReference, SessionState.ExportObject<Table>> resolveReference) {
            dependencies = operation.getTableReferences(request).stream()
                .map(resolveReference)
                .collect(Collectors.toList());
            exportBuilder.require(dependencies);
        }

        Table doExport() {
            return operation.create(request, dependencies);
        }
    }
}
