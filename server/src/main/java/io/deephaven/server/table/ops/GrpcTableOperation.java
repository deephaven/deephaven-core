/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import io.deephaven.auth.AuthContext;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.server.session.SessionState;
import io.grpc.StatusRuntimeException;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class GrpcTableOperation<T> {
    @FunctionalInterface
    protected interface MultiDependencyFunction<T> {
        List<TableReference> getTableReferences(T request);
    }
    @FunctionalInterface
    protected interface PermissionFunction<T> {
        void check(AuthContext authContext, T request, List<Table> sourceTables);
    }

    private final PermissionFunction<T> permission;
    private final Function<BatchTableRequest.Operation, T> getRequest;
    private final Function<T, Ticket> getTicket;
    private final MultiDependencyFunction<T> getDependencies;

    /**
     * This table operation has many dependencies.
     *
     * @param permission a function to validate the request given the auth context, source tables and request
     * @param getRequest a functor to extract the request from a BatchTableRequest.Operation
     * @param getTicket a function to extract the result ticket from the request
     * @param getDependencies a function to extract the table-reference dependencies from the request
     */
    protected GrpcTableOperation(
            final PermissionFunction<T> permission,
            final Function<BatchTableRequest.Operation, T> getRequest,
            final Function<T, Ticket> getTicket,
            final MultiDependencyFunction<T> getDependencies) {
        this.permission = permission;
        this.getRequest = getRequest;
        this.getTicket = getTicket;
        this.getDependencies = getDependencies;
    }

    /**
     * This table operation has one dependency.
     *
     * @param permission a function to validate the request given the auth context, source tables and request
     * @param getRequest a function to extract the request from a BatchTableRequest.Operation
     * @param getTicket a function to extract the result ticket from the request
     * @param getDependency a function to extract the table-reference dependency from the request
     */
    protected GrpcTableOperation(
            final PermissionFunction<T> permission,
            final Function<BatchTableRequest.Operation, T> getRequest,
            final Function<T, Ticket> getTicket,
            final Function<T, TableReference> getDependency) {
        this.permission = permission;
        this.getRequest = getRequest;
        this.getTicket = getTicket;
        this.getDependencies = (request) -> Collections.singletonList(getDependency.apply(request));
    }

    /**
     * This table operation has no dependencies.
     *
     * @param permission a function to validate the request given the auth context, source tables and request
     * @param getRequest a functor to extract the request from a BatchTableRequest.Operation
     * @param getTicket a function to extract the result ticket from the request
     */
    protected GrpcTableOperation(
            final PermissionFunction<T> permission,
            final Function<BatchTableRequest.Operation, T> getRequest,
            final Function<T, Ticket> getTicket) {
        this.permission = permission;
        this.getRequest = getRequest;
        this.getTicket = getTicket;
        this.getDependencies = (request) -> Collections.emptyList();
    }

    /**
     * This method validates preconditions of the request.
     *
     * @param request the original request from the user
     * @throws StatusRuntimeException on the first failed precondition
     */
    public void validateRequest(final T request) throws StatusRuntimeException {
        // many operations cannot do validation without the parent tables being resolved first
    }

    /**
     * This actually performs the operation. It will typically be performed after the
     *
     * @param request the original request from the user
     * @param sourceTables the source tables that this operation may or may not need
     * @return the resulting table
     */
    abstract public Table create(T request, List<SessionState.ExportObject<Table>> sourceTables);

    /**
     * Extract the specific request object from the batch operation.
     *
     * @param op the batch operation
     * @return the typed request from the batch
     */
    public T getRequestFromOperation(final BatchTableRequest.Operation op) {
        return getRequest.apply(op);
    }

    /**
     * Get the result ticket for this operation.
     *
     * @param request the request
     * @return the result ticket
     */
    public Ticket getResultTicket(final T request) {
        return getTicket.apply(request);
    }

    /**
     * Get the table references for this operation.
     *
     * @param request the request
     * @return the table references of the other source table dependencies
     */
    public List<TableReference> getTableReferences(final T request) {
        return getDependencies.getTableReferences(request);
    }

    /**
     * Verify that this request is allowed to be performed.
     *
     * @param request the original request from the user
     * @param sourceTables the source tables that this operation may or may not need
     */
    public void checkPermission(final T request, final List<SessionState.ExportObject<Table>> sourceTables) {
        final List<Table> tables =
                sourceTables.stream().map(SessionState.ExportObject::get).collect(Collectors.toList());
        permission.check(ExecutionContext.getContext().getAuthContext(), request, tables);
    }
}
