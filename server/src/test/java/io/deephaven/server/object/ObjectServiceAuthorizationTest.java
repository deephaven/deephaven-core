//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.object;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.plugin.type.Exporter;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeClassBase;
import io.deephaven.proto.backplane.grpc.ConnectRequest;
import io.deephaven.proto.backplane.grpc.StreamRequest;
import io.deephaven.proto.backplane.grpc.StreamResponse;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.runner.DeephavenApiServerSingleAuthenticatedBase;
import io.deephaven.server.session.NoopTicketResolverAuthorization;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.server.test.TestAuthorizationProvider;
import io.grpc.stub.StreamObserver;
import org.junit.Test;

import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * Verifies that the server applies the authorization transform to references a plugin exports via
 * {@link ObjectType.MessageStream#onData}, gated by the plugin's {@link ObjectType#authorizationExportBehavior()
 * declared behavior}.
 */
public class ObjectServiceAuthorizationTest extends DeephavenApiServerSingleAuthenticatedBase {

    private final TestAuthorizationProvider authorizationProvider = new TestAuthorizationProvider();

    @Override
    protected AuthorizationProvider authorizationProvider() {
        return authorizationProvider;
    }

    /** Drops the {@code "J"} column from any exported {@link Table}, counting each transform. */
    private AtomicInteger installDropJTransform() {
        final AtomicInteger transforms = new AtomicInteger();
        authorizationProvider.delegateTicketTransformation = new NoopTicketResolverAuthorization() {
            @Override
            public <T> T transform(T source) {
                if (source instanceof Table) {
                    transforms.incrementAndGet();
                    // noinspection unchecked
                    return (T) ((Table) source).dropColumns("J");
                }
                return source;
            }
        };
        return transforms;
    }

    @Test
    public void transformOptInTransformsExportedReference()
            throws ExecutionException, InterruptedException, TimeoutException {
        final AtomicInteger transforms = installDropJTransform();

        final StreamResponse response = connectAndAwaitData(
                TransformHolder.class.getName(),
                new TransformHolder(twoColumnTable()));

        assertThat(transforms.get()).isEqualTo(1);
        assertThat(response.getData().getExportedReferencesCount()).isEqualTo(1);

        final TypedTicket ref = response.getData().getExportedReferences(0);
        assertThat(ref.getType()).isEqualTo("Table");
        final Table transformed = authenticatedSessionState().<Table>getExport(ref.getTicket(), "ref").get();
        assertThat(transformed.getColumnSourceMap().keySet()).containsExactly("I");
    }

    @Test
    public void transformDeniedFailsStream() throws InterruptedException, TimeoutException {
        authorizationProvider.delegateTicketTransformation = new NoopTicketResolverAuthorization() {
            @Override
            public <T> T transform(T source) {
                return source instanceof Table ? null : source;
            }
        };

        final CompletableFuture<StreamResponse> cf =
                connect(TransformHolder.class.getName(), new TransformHolder(twoColumnTable()));
        try {
            cf.get(5, TimeUnit.SECONDS);
            failBecauseExceptionWasNotThrown(ExecutionException.class);
        } catch (ExecutionException expected) {
            // the stream fails because the user is not authorized to access the reference
        }
    }

    @Test
    public void manualOptOutDoesNotTransform()
            throws ExecutionException, InterruptedException, TimeoutException {
        final AtomicInteger transforms = installDropJTransform();

        final StreamResponse response = connectAndAwaitData(
                ManualHolder.class.getName(),
                new ManualHolder(twoColumnTable()));

        assertThat(transforms.get()).isZero();
        final TypedTicket ref = response.getData().getExportedReferences(0);
        final Table untransformed = authenticatedSessionState().<Table>getExport(ref.getTicket(), "ref").get();
        assertThat(untransformed.getColumnSourceMap().keySet()).containsExactly("I", "J");
    }

    @Test
    public void unsetUnderPermissivePolicyDoesNotTransform()
            throws ExecutionException, InterruptedException, TimeoutException {
        // The default policy is PERMISSIVE, so an undeclared (UNSET) plugin is served without transforming.
        final AtomicInteger transforms = installDropJTransform();

        final StreamResponse response = connectAndAwaitData(
                UnsetHolder.class.getName(),
                new UnsetHolder(twoColumnTable()));

        assertThat(transforms.get()).isZero();
        final TypedTicket ref = response.getData().getExportedReferences(0);
        final Table untransformed = authenticatedSessionState().<Table>getExport(ref.getTicket(), "ref").get();
        assertThat(untransformed.getColumnSourceMap().keySet()).containsExactly("I", "J");
    }

    private static Table twoColumnTable() {
        return TableTools.emptyTable(10).update("I = i", "J = i + 1");
    }

    private StreamResponse connectAndAwaitData(String type, Object holder)
            throws ExecutionException, InterruptedException, TimeoutException {
        return connect(type, holder).get(5, TimeUnit.SECONDS);
    }

    private <T> CompletableFuture<StreamResponse> connect(String type, T holder) {
        final ExportObject<T> export = authenticatedSessionState().<T>newExport(1).submit(() -> holder);
        final CompletableFuture<StreamResponse> cf = new CompletableFuture<>();
        final StreamObserver<StreamRequest> observer = channel().object().messageStream(new StreamObserver<>() {
            @Override
            public void onNext(StreamResponse value) {
                cf.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                cf.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                if (!cf.isDone()) {
                    cf.completeExceptionally(new RuntimeException("Expected future to complete"));
                }
            }
        });
        observer.onNext(StreamRequest.newBuilder()
                .setConnect(ConnectRequest.newBuilder()
                        .setSourceId(TypedTicket.newBuilder().setType(type).setTicket(export.getExportId())))
                .build());
        observer.onCompleted();
        return cf;
    }

    public static class TransformHolder {
        private final Table table;

        public TransformHolder(Table table) {
            this.table = table;
        }
    }

    public static class ManualHolder {
        private final Table table;

        public ManualHolder(Table table) {
            this.table = table;
        }
    }

    public static class UnsetHolder {
        private final Table table;

        public UnsetHolder(Table table) {
            this.table = table;
        }
    }

    @AutoService(ObjectType.class)
    public static class TransformHolderType extends ObjectTypeClassBase.FetchOnly<TransformHolder> {
        public TransformHolderType() {
            super(TransformHolder.class.getName(), TransformHolder.class);
        }

        @Override
        public AuthorizationExportBehavior authorizationExportBehavior() {
            return AuthorizationExportBehavior.TRANSFORM;
        }

        @Override
        public void writeToImpl(Exporter exporter, TransformHolder object, OutputStream out) {
            exporter.reference(object.table);
        }
    }

    @AutoService(ObjectType.class)
    public static class ManualHolderType extends ObjectTypeClassBase.FetchOnly<ManualHolder> {
        public ManualHolderType() {
            super(ManualHolder.class.getName(), ManualHolder.class);
        }

        @Override
        public AuthorizationExportBehavior authorizationExportBehavior() {
            return AuthorizationExportBehavior.MANUAL;
        }

        @Override
        public void writeToImpl(Exporter exporter, ManualHolder object, OutputStream out) {
            exporter.reference(object.table);
        }
    }

    @AutoService(ObjectType.class)
    public static class UnsetHolderType extends ObjectTypeClassBase.FetchOnly<UnsetHolder> {
        public UnsetHolderType() {
            super(UnsetHolder.class.getName(), UnsetHolder.class);
        }

        @Override
        public void writeToImpl(Exporter exporter, UnsetHolder object, OutputStream out) {
            exporter.reference(object.table);
        }
    }
}
