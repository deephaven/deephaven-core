/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.clientsupport.gotorow.SeekRow;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.engine.table.Table;
import io.deephaven.extensions.barrage.util.ExportUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.AggregateAllRequest;
import io.deephaven.proto.backplane.grpc.AggregateRequest;
import io.deephaven.proto.backplane.grpc.ApplyPreviewColumnsRequest;
import io.deephaven.proto.backplane.grpc.AsOfJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.AjRajTablesRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation.OpCase;
import io.deephaven.proto.backplane.grpc.ColumnStatisticsRequest;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest;
import io.deephaven.proto.backplane.grpc.CrossJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.DropColumnsRequest;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.ExactJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateMessage;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdatesRequest;
import io.deephaven.proto.backplane.grpc.FetchTableRequest;
import io.deephaven.proto.backplane.grpc.FilterTableRequest;
import io.deephaven.proto.backplane.grpc.FlattenRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailByRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailRequest;
import io.deephaven.proto.backplane.grpc.LeftJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.Literal;
import io.deephaven.proto.backplane.grpc.MergeTablesRequest;
import io.deephaven.proto.backplane.grpc.MetaTableRequest;
import io.deephaven.proto.backplane.grpc.NaturalJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.RunChartDownsampleRequest;
import io.deephaven.proto.backplane.grpc.SeekRowRequest;
import io.deephaven.proto.backplane.grpc.SeekRowResponse;
import io.deephaven.proto.backplane.grpc.SelectDistinctRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.SnapshotTableRequest;
import io.deephaven.proto.backplane.grpc.SnapshotWhenTableRequest;
import io.deephaven.proto.backplane.grpc.SortTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import io.deephaven.proto.backplane.grpc.UngroupRequest;
import io.deephaven.proto.backplane.grpc.UnstructuredFilterTableRequest;
import io.deephaven.proto.backplane.grpc.UpdateByRequest;
import io.deephaven.proto.backplane.grpc.WhereInRequest;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.grpc.GrpcErrorHelper;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.SessionState.ExportBuilder;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.server.table.ExportedTableUpdateListener;
import io.deephaven.time.DateTimeUtils;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyComplete;
import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyError;
import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyOnNext;

public class TableServiceGrpcImpl extends TableServiceGrpc.TableServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(TableServiceGrpcImpl.class);

    private final TicketRouter ticketRouter;
    private final SessionService sessionService;
    private final TableServiceContextualAuthWiring authWiring;
    private final Map<BatchTableRequest.Operation.OpCase, GrpcTableOperation<?>> operationMap;

    @Inject
    public TableServiceGrpcImpl(final TicketRouter ticketRouter,
            final SessionService sessionService,
            final TableServiceContextualAuthWiring authWiring,
            final Map<BatchTableRequest.Operation.OpCase, GrpcTableOperation<?>> operationMap) {
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
        this.authWiring = authWiring;
        this.operationMap = operationMap;
    }

    private <T> GrpcTableOperation<T> getOp(final BatchTableRequest.Operation.OpCase op) {
        // noinspection unchecked
        final GrpcTableOperation<T> operation = (GrpcTableOperation<T>) operationMap.get(op);
        if (operation == null) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "BatchTableRequest.Operation.OpCode is unset, incompatible, or not yet supported. (found: " + op
                            + ")");
        }
        return operation;
    }

    @Override
    public void emptyTable(
            @NotNull final EmptyTableRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.EMPTY_TABLE, request, responseObserver);
    }

    @Override
    public void timeTable(
            @NotNull final TimeTableRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.TIME_TABLE, request, responseObserver);
    }

    @Override
    public void mergeTables(
            @NotNull final MergeTablesRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.MERGE, request, responseObserver);
    }

    @Override
    public void selectDistinct(
            @NotNull final SelectDistinctRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.SELECT_DISTINCT, request, responseObserver);
    }

    @Override
    public void update(
            @NotNull final SelectOrUpdateRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.UPDATE, request, responseObserver);
    }

    @Override
    public void lazyUpdate(
            @NotNull final SelectOrUpdateRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.LAZY_UPDATE, request, responseObserver);
    }

    @Override
    public void view(
            @NotNull final SelectOrUpdateRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.VIEW, request, responseObserver);
    }

    @Override
    public void updateView(
            @NotNull final SelectOrUpdateRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.UPDATE_VIEW, request, responseObserver);
    }

    @Override
    public void select(
            @NotNull final SelectOrUpdateRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.SELECT, request, responseObserver);
    }

    @Override
    public void headBy(
            @NotNull final HeadOrTailByRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.HEAD_BY, request, responseObserver);
    }

    @Override
    public void tailBy(
            @NotNull final HeadOrTailByRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.TAIL_BY, request, responseObserver);
    }

    @Override
    public void head(
            @NotNull final HeadOrTailRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.HEAD, request, responseObserver);
    }

    @Override
    public void tail(
            @NotNull final HeadOrTailRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.TAIL, request, responseObserver);
    }

    @Override
    public void ungroup(
            @NotNull final UngroupRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.UNGROUP, request, responseObserver);
    }

    @Override
    public void comboAggregate(
            @NotNull final ComboAggregateRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.COMBO_AGGREGATE, request, responseObserver);
    }

    @Override
    public void aggregateAll(
            @NotNull final AggregateAllRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(OpCase.AGGREGATE_ALL, request, responseObserver);
    }

    @Override
    public void aggregate(
            @NotNull final AggregateRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.AGGREGATE, request, responseObserver);
    }

    @Override
    public void snapshot(
            @NotNull final SnapshotTableRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.SNAPSHOT, request, responseObserver);
    }

    @Override
    public void snapshotWhen(
            @NotNull final SnapshotWhenTableRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.SNAPSHOT_WHEN, request, responseObserver);
    }

    @Override
    public void dropColumns(
            @NotNull final DropColumnsRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.DROP_COLUMNS, request, responseObserver);
    }

    @Override
    public void filter(
            @NotNull final FilterTableRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.FILTER, request, responseObserver);
    }

    @Override
    public void unstructuredFilter(
            @NotNull final UnstructuredFilterTableRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.UNSTRUCTURED_FILTER, request, responseObserver);
    }

    @Override
    public void sort(
            @NotNull final SortTableRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.SORT, request, responseObserver);
    }

    @Override
    public void flatten(
            @NotNull final FlattenRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.FLATTEN, request, responseObserver);
    }

    @Override
    public void metaTable(
            @NotNull final MetaTableRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(OpCase.META_TABLE, request, responseObserver);
    }

    @Override
    public void crossJoinTables(
            @NotNull final CrossJoinTablesRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.CROSS_JOIN, request, responseObserver);
    }

    @Override
    public void naturalJoinTables(
            @NotNull final NaturalJoinTablesRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.NATURAL_JOIN, request, responseObserver);
    }

    @Override
    public void exactJoinTables(
            @NotNull final ExactJoinTablesRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.EXACT_JOIN, request, responseObserver);
    }

    @Override
    public void leftJoinTables(LeftJoinTablesRequest request,
            StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.LEFT_JOIN, request, responseObserver);
    }

    @Override
    public void asOfJoinTables(AsOfJoinTablesRequest request,
            StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.AS_OF_JOIN, request, responseObserver);
    }

    @Override
    public void ajTables(AjRajTablesRequest request, StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.AJ, request, responseObserver);
    }

    @Override
    public void rajTables(AjRajTablesRequest request, StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.RAJ, request, responseObserver);
    }

    @Override
    public void rangeJoinTables(RangeJoinTablesRequest request,
            StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.RANGE_JOIN, request, responseObserver);
    }

    @Override
    public void runChartDownsample(RunChartDownsampleRequest request,
            StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.RUN_CHART_DOWNSAMPLE, request, responseObserver);
    }

    @Override
    public void fetchTable(FetchTableRequest request, StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.FETCH_TABLE, request, responseObserver);
    }

    @Override
    public void applyPreviewColumns(ApplyPreviewColumnsRequest request,
            StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.APPLY_PREVIEW_COLUMNS, request, responseObserver);
    }

    @Override
    public void createInputTable(CreateInputTableRequest request,
            StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.CREATE_INPUT_TABLE, request, responseObserver);
    }

    @Override
    public void updateBy(UpdateByRequest request, StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.UPDATE_BY, request, responseObserver);
    }

    private Object getSeekValue(Literal literal, Class<?> dataType) {
        if (literal.hasStringValue()) {
            if (BigDecimal.class.isAssignableFrom(dataType)) {
                return new BigDecimal(literal.getStringValue());
            }
            if (BigInteger.class.isAssignableFrom(dataType)) {
                return new BigInteger(literal.getStringValue());
            }
            if (!String.class.isAssignableFrom(dataType) && dataType != char.class) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Invalid String type for seek: " + dataType);
            }
            return literal.getStringValue();
        } else if (literal.hasNanoTimeValue()) {
            if (!Instant.class.isAssignableFrom(dataType)) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Invalid date type for seek: " + dataType);
            }
            return DateTimeUtils.epochNanosToInstant(literal.getNanoTimeValue());
        } else if (literal.hasLongValue()) {
            Long longValue = literal.getLongValue();
            if (dataType == byte.class) {
                return longValue.byteValue();
            }
            if (dataType == short.class) {
                return longValue.shortValue();
            }
            if (dataType == int.class) {
                return longValue.intValue();
            }
            if (dataType == long.class) {
                return longValue;
            }
            if (dataType == float.class) {
                return longValue.floatValue();
            }
            if (dataType == double.class) {
                return longValue.doubleValue();
            }
        } else if (literal.hasDoubleValue()) {
            Double doubleValue = literal.getDoubleValue();
            if (dataType == byte.class) {
                return doubleValue.byteValue();
            }
            if (dataType == short.class) {
                return doubleValue.shortValue();
            }
            if (dataType == int.class) {
                return doubleValue.intValue();
            }
            if (dataType == long.class) {
                return doubleValue.longValue();
            }
            if (dataType == float.class) {
                return doubleValue.floatValue();
            }
            if (dataType == double.class) {
                return doubleValue;
            }
        } else if (literal.hasBoolValue()) {
            return literal.getBoolValue();
        }
        throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Invalid column type for seek: " + dataType);
    }

    @Override
    public void whereIn(
            @NotNull final WhereInRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.WHERE_IN, request, responseObserver);
    }

    @Override
    public void seekRow(
            @NotNull final SeekRowRequest request,
            @NotNull final StreamObserver<SeekRowResponse> responseObserver) {
        final SessionState session = sessionService.getCurrentSession();
        final Ticket sourceId = request.getSourceId();
        if (sourceId.getTicket().isEmpty()) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "No consoleId supplied");
        }
        SessionState.ExportObject<Table> exportedTable =
                ticketRouter.resolve(session, sourceId, "sourceId");
        session.nonExport()
                .require(exportedTable)
                .onError(responseObserver)
                .submit(() -> {
                    final Table table = exportedTable.get();
                    authWiring.checkPermissionSeekRow(session.getAuthContext(), request,
                            Collections.singletonList(table));
                    final String columnName = request.getColumnName();
                    final Class<?> dataType = table.getDefinition().getColumn(columnName).getDataType();
                    final Object seekValue = getSeekValue(request.getSeekValue(), dataType);
                    final Long result = table.apply(new SeekRow(
                            request.getStartingRow(),
                            columnName,
                            seekValue,
                            request.getInsensitive(),
                            request.getContains(),
                            request.getIsBackward()));
                    SeekRowResponse.Builder rowResponse = SeekRowResponse.newBuilder();
                    safelyComplete(responseObserver, rowResponse.setResultRow(result).build());
                });
    }

    @Override
    public void computeColumnStatistics(ColumnStatisticsRequest request,
            StreamObserver<ExportedTableCreationResponse> responseObserver) {
        oneShotOperationWrapper(BatchTableRequest.Operation.OpCase.COLUMN_STATISTICS, request, responseObserver);
    }

    @Override
    public void batch(
            @NotNull final BatchTableRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        GrpcErrorHelper.checkRepeatedFieldNonEmpty(request, BatchTableRequest.OPS_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        for (Operation operation : request.getOpsList()) {
            GrpcErrorHelper.checkHasOneOf(operation, "op");
            GrpcErrorHelper.checkHasNoUnknownFields(operation);
        }
        final SessionState session = sessionService.getCurrentSession();

        // step 1: initialize exports
        final List<BatchExportBuilder<?>> exportBuilders = request.getOpsList().stream()
                .map(op -> createBatchExportBuilder(session, op))
                .collect(Collectors.toList());

        // step 2: resolve dependencies
        exportBuilders.forEach(export -> export.resolveDependencies(session, exportBuilders));

        // step 3: check for cyclical dependencies; this is our only opportunity to check non-export cycles
        // TODO: check for cycles

        // step 4: submit the batched operations
        final AtomicInteger remaining = new AtomicInteger(exportBuilders.size());
        final AtomicReference<StatusRuntimeException> firstFailure = new AtomicReference<>();

        final Runnable onOneResolved = () -> {
            if (remaining.decrementAndGet() == 0) {
                final StatusRuntimeException failure = firstFailure.get();
                if (failure != null) {
                    safelyError(responseObserver, failure);
                } else {
                    safelyComplete(responseObserver);
                }
            }
        };

        for (int i = 0; i < exportBuilders.size(); ++i) {
            final BatchExportBuilder<?> exportBuilder = exportBuilders.get(i);
            final int exportId = exportBuilder.exportBuilder.getExportId();

            final TableReference resultId;
            if (exportId == SessionState.NON_EXPORT_ID) {
                resultId = TableReference.newBuilder().setBatchOffset(i).build();
            } else {
                resultId = ExportTicketHelper.tableReference(exportId);
            }

            exportBuilder.exportBuilder.onError((result, errorContext, cause, dependentId) -> {
                String errorInfo = errorContext;
                if (dependentId != null) {
                    errorInfo += " dependency: " + dependentId;
                }
                if (cause instanceof StatusRuntimeException) {
                    errorInfo += " cause: " + cause.getMessage();
                    firstFailure.compareAndSet(null, (StatusRuntimeException) cause);
                }
                final ExportedTableCreationResponse response = ExportedTableCreationResponse.newBuilder()
                        .setResultId(resultId)
                        .setSuccess(false)
                        .setErrorInfo(errorInfo)
                        .build();
                safelyOnNext(responseObserver, response);
                onOneResolved.run();
            }).submit(() -> {
                final Table table = exportBuilder.doExport();
                final ExportedTableCreationResponse response =
                        ExportUtil.buildTableCreationResponse(resultId, table);
                safelyOnNext(responseObserver, response);
                onOneResolved.run();
                return table;
            });
        }
    }

    @Override
    public void exportedTableUpdates(
            @NotNull final ExportedTableUpdatesRequest request,
            @NotNull final StreamObserver<ExportedTableUpdateMessage> responseObserver) {
        final SessionState session = sessionService.getCurrentSession();
        authWiring.checkPermissionExportedTableUpdates(session.getAuthContext(), request, Collections.emptyList());
        final ExportedTableUpdateListener listener = new ExportedTableUpdateListener(session, responseObserver);
        session.addExportListener(listener);
        ((ServerCallStreamObserver<ExportedTableUpdateMessage>) responseObserver).setOnCancelHandler(
                () -> session.removeExportListener(listener));
    }

    @Override
    public void getExportedTableCreationResponse(
            @NotNull final Ticket request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        final SessionState session = sessionService.getCurrentSession();

        if (request.getTicket().isEmpty()) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "No request ticket supplied");
        }

        final SessionState.ExportObject<Object> export = ticketRouter.resolve(session, request, "request");

        session.nonExport()
                .require(export)
                .onError(responseObserver)
                .submit(() -> {
                    final Object obj = export.get();
                    if (!(obj instanceof Table)) {
                        responseObserver.onError(
                                Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                                        "Ticket is not a table"));
                        return;
                    }
                    authWiring.checkPermissionGetExportedTableCreationResponse(
                            session.getAuthContext(), request, Collections.singletonList((Table) obj));
                    final ExportedTableCreationResponse response =
                            ExportUtil.buildTableCreationResponse(request, (Table) obj);
                    safelyComplete(responseObserver, response);
                });
    }

    /**
     * This helper is a wrapper that enables one-shot RPCs to utilize the same code paths that a batch RPC utilizes.
     *
     * @param op the protobuf op-code for the batch operation request
     * @param request the protobuf that is mapped to this op-code
     * @param responseObserver the observer that needs to know the result of this rpc
     * @param <T> the protobuf type that configures the behavior of the operation
     */
    private <T> void oneShotOperationWrapper(
            final BatchTableRequest.Operation.OpCase op,
            final T request,
            final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        final SessionState session = sessionService.getCurrentSession();
        final GrpcTableOperation<T> operation = getOp(op);
        operation.validateRequest(request);

        final Ticket resultId = operation.getResultTicket(request);
        if (resultId.getTicket().isEmpty()) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "No result ticket supplied");
        }

        final List<SessionState.ExportObject<Table>> dependencies = operation.getTableReferences(request).stream()
                .map(ref -> resolveOneShotReference(session, ref))
                .collect(Collectors.toList());

        session.newExport(resultId, "resultId")
                .require(dependencies)
                .onError(responseObserver)
                .submit(() -> {
                    operation.checkPermission(request, dependencies);
                    final Table result = operation.create(request, dependencies);
                    final ExportedTableCreationResponse response =
                            ExportUtil.buildTableCreationResponse(resultId, result);
                    safelyComplete(responseObserver, response);
                    return result;
                });
    }

    private SessionState.ExportObject<Table> resolveOneShotReference(SessionState session, TableReference ref) {
        if (!ref.hasTicket()) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "One-shot operations must use ticket references");
        }
        return ticketRouter.resolve(session, ref.getTicket(), "sourceId");
    }

    private SessionState.ExportObject<Table> resolveBatchReference(SessionState session,
            List<BatchExportBuilder<?>> exportBuilders, TableReference ref) {
        switch (ref.getRefCase()) {
            case TICKET:
                return ticketRouter.resolve(session, ref.getTicket(), "sourceId");
            case BATCH_OFFSET:
                final int offset = ref.getBatchOffset();
                if (offset < 0 || offset >= exportBuilders.size()) {
                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "invalid table reference: " + ref);
                }
                return exportBuilders.get(offset).exportBuilder.getExport();
            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "invalid table reference: " + ref);
        }
    }

    private <T> BatchExportBuilder<T> createBatchExportBuilder(SessionState session, BatchTableRequest.Operation op) {
        final GrpcTableOperation<T> operation = getOp(op.getOpCase());
        final T request = operation.getRequestFromOperation(op);
        operation.validateRequest(request);

        final Ticket resultId = operation.getResultTicket(request);
        final ExportBuilder<Table> exportBuilder =
                resultId.getTicket().isEmpty() ? session.nonExport() : session.newExport(resultId, "resultId");
        return new BatchExportBuilder<>(operation, request, exportBuilder);
    }

    private class BatchExportBuilder<T> {
        private final GrpcTableOperation<T> operation;
        private final T request;
        private final SessionState.ExportBuilder<Table> exportBuilder;

        List<SessionState.ExportObject<Table>> dependencies;

        BatchExportBuilder(GrpcTableOperation<T> operation, T request, ExportBuilder<Table> exportBuilder) {
            this.operation = Objects.requireNonNull(operation);
            this.request = Objects.requireNonNull(request);
            this.exportBuilder = Objects.requireNonNull(exportBuilder);
        }

        void resolveDependencies(SessionState session, List<BatchExportBuilder<?>> exportBuilders) {
            dependencies = operation.getTableReferences(request).stream()
                    .map(ref -> resolveBatchReference(session, exportBuilders, ref))
                    .collect(Collectors.toList());
            exportBuilder.require(dependencies);
        }

        Table doExport() {
            operation.checkPermission(request, dependencies);
            return operation.create(request, dependencies);
        }
    }
}
