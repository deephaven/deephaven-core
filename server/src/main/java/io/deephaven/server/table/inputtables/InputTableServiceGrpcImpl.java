//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.inputtables;

import com.google.rpc.Code;
import com.google.protobuf.Any;
import io.grpc.protobuf.StatusProto;
import io.deephaven.auth.codegen.impl.InputTableServiceContextualAuthWiring;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.util.input.InputTableStatusListener;
import io.deephaven.engine.util.input.InputTableUpdater;
import io.deephaven.engine.util.input.InputTableValidationException;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import java.lang.Object;
import java.util.Collection;
import java.util.List;

public class InputTableServiceGrpcImpl extends InputTableServiceGrpc.InputTableServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(InputTableServiceGrpcImpl.class);

    private final InputTableServiceContextualAuthWiring authWiring;
    private final TicketRouter ticketRouter;
    private final SessionService sessionService;

    @Inject
    public InputTableServiceGrpcImpl(
            final InputTableServiceContextualAuthWiring authWiring,
            final TicketRouter ticketRouter,
            final SessionService sessionService) {
        this.authWiring = authWiring;
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
    }

    @Override
    public void addTableToInputTable(
            @NotNull final AddTableRequest request,
            @NotNull final StreamObserver<AddTableResponse> responseObserver) {
        final SessionState session = sessionService.getCurrentSession();

        final String description = "InputTableService#addTableToInputTable(inputTable="
                + ticketRouter.getLogNameFor(request.getInputTable(), "inputTable") + ", tableToAdd="
                + ticketRouter.getLogNameFor(request.getTableToAdd(), "tableToAdd") + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportObject<Table> targetTable =
                    ticketRouter.resolve(session, request.getInputTable(), "inputTable");

            final SessionState.ExportObject<Table> tableToAddExport =
                    ticketRouter.resolve(session, request.getTableToAdd(), "tableToAdd");

            session.nonExport()
                    .queryPerformanceRecorder(queryPerformanceRecorder)
                    .onError(responseObserver)
                    .require(targetTable, tableToAddExport)
                    .submit(() -> {
                        Object inputTableAsObject = targetTable.get().getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
                        if (!(inputTableAsObject instanceof InputTableUpdater)) {
                            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Table can't be used as an input table");
                        }

                        final InputTableUpdater inputTableUpdater = (InputTableUpdater) inputTableAsObject;
                        Table tableToAdd = tableToAddExport.get();

                        authWiring.checkPermissionAddTableToInputTable(
                                ExecutionContext.getContext().getAuthContext(), request,
                                List.of(targetTable.get(), tableToAdd));

                        // validate that the columns are compatible
                        try {
                            inputTableUpdater.validateAddOrModify(tableToAdd);
                        } catch (TableDefinition.IncompatibleTableDefinitionException exception) {
                            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Provided tables's columns are not compatible: " + exception.getMessage());
                        } catch (InputTableValidationException exception) {
                            final Collection<InputTableValidationException.StructuredError> errors =
                                    exception.getErrors();
                            if (errors.isEmpty()) {
                                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                        "Invalid update request: " + exception.getMessage());
                            } else {
                                com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                                        .setCode(Code.INVALID_ARGUMENT.getNumber())
                                        .setMessage(exception.getMessage())
                                        .addDetails(Any.pack(convertErrors(errors)))
                                        .build();

                                GrpcUtil.safelyError(responseObserver, StatusProto.toStatusException(status));
                                return;
                            }
                        }

                        // actually add the tables contents
                        inputTableUpdater.addAsync(tableToAdd, new InputTableStatusListener() {
                            @Override
                            public void onSuccess() {
                                GrpcUtil.safelyOnNextAndComplete(responseObserver,
                                        AddTableResponse.getDefaultInstance());
                            }

                            @Override
                            public void onError(Throwable t) {
                                if (t instanceof InputTableValidationException) {
                                    final Collection<InputTableValidationException.StructuredError> errors =
                                            ((InputTableValidationException) t).getErrors();
                                    if (errors.isEmpty()) {
                                        GrpcUtil.safelyError(responseObserver,
                                                Exceptions.statusRuntimeException(Code.DATA_LOSS,
                                                        "Error adding table to input table: " + t.getMessage()));
                                    } else {
                                        com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                                                .setCode(Code.DATA_LOSS.getNumber())
                                                .setMessage(t.getMessage())
                                                .addDetails(Any.pack(convertErrors(errors)))
                                                .build();

                                        GrpcUtil.safelyError(responseObserver, StatusProto.toStatusException(status));
                                    }
                                } else {
                                    GrpcUtil.safelyError(responseObserver,
                                            Exceptions.statusRuntimeException(Code.DATA_LOSS,
                                                    "Error adding table to input table"));
                                }
                            }
                        });
                    });
        }
    }

    private InputTableValidationErrorList convertErrors(
            final Collection<InputTableValidationException.StructuredError> errors) {
        final InputTableValidationErrorList.Builder responseBuilder = InputTableValidationErrorList.newBuilder();
        errors.forEach(e -> responseBuilder.addValidationErrors(convertOneError(e)));
        return responseBuilder.build();
    }

    private InputTableValidationError convertOneError(final InputTableValidationException.StructuredError e) {
        final InputTableValidationError.Builder errorBuilder = InputTableValidationError.newBuilder();
        errorBuilder.setMessage(e.getMessage());
        e.getRow().ifPresent(errorBuilder::setRow);
        e.getColumn().ifPresent(errorBuilder::setColumn);
        return errorBuilder.build();
    }

    @Override
    public void deleteTableFromInputTable(
            @NotNull final DeleteTableRequest request,
            @NotNull final StreamObserver<DeleteTableResponse> responseObserver) {
        final SessionState session = sessionService.getCurrentSession();

        final String description = "InputTableService#deleteTableFromInputTable(inputTable="
                + ticketRouter.getLogNameFor(request.getInputTable(), "inputTable") + ", tableToRemove="
                + ticketRouter.getLogNameFor(request.getTableToRemove(), "tableToRemove") + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportObject<Table> targetTable =
                    ticketRouter.resolve(session, request.getInputTable(), "inputTable");

            final SessionState.ExportObject<Table> tableToRemoveExport =
                    ticketRouter.resolve(session, request.getTableToRemove(), "tableToRemove");

            session.nonExport()
                    .queryPerformanceRecorder(queryPerformanceRecorder)
                    .onError(responseObserver)
                    .require(targetTable, tableToRemoveExport)
                    .submit(() -> {
                        Object inputTableAsObject = targetTable.get().getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
                        if (!(inputTableAsObject instanceof InputTableUpdater)) {
                            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Table can't be used as an input table");
                        }

                        final InputTableUpdater inputTableUpdater = (InputTableUpdater) inputTableAsObject;
                        Table tableToRemove = tableToRemoveExport.get();

                        authWiring.checkPermissionDeleteTableFromInputTable(
                                ExecutionContext.getContext().getAuthContext(), request,
                                List.of(targetTable.get(), tableToRemove));

                        // validate that the columns are compatible
                        try {
                            inputTableUpdater.validateDelete(tableToRemove);
                        } catch (TableDefinition.IncompatibleTableDefinitionException exception) {
                            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Provided tables's columns are not compatible: " + exception.getMessage());
                        } catch (UnsupportedOperationException exception) {
                            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Provided input table does not support delete.");
                        } catch (InputTableValidationException exception) {
                            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Invalid update request: " + exception.getMessage());
                        }

                        // actually delete the table's contents
                        inputTableUpdater.deleteAsync(tableToRemove, new InputTableStatusListener() {
                            @Override
                            public void onSuccess() {
                                GrpcUtil.safelyOnNextAndComplete(responseObserver,
                                        DeleteTableResponse.getDefaultInstance());
                            }

                            @Override
                            public void onError(Throwable t) {
                                if (t instanceof InputTableValidationException) {
                                    GrpcUtil.safelyError(responseObserver,
                                            Exceptions.statusRuntimeException(Code.DATA_LOSS,
                                                    "Error deleting table from input table: " + t.getMessage()));
                                } else {
                                    GrpcUtil.safelyError(responseObserver,
                                            Exceptions.statusRuntimeException(Code.DATA_LOSS,
                                                    "Error deleting table from inputtable"));
                                }
                            }
                        });
                    });
        }
    }
}
