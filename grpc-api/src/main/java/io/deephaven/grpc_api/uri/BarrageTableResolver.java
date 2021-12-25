package io.deephaven.grpc_api.uri;

import io.deephaven.client.impl.BarrageSession;
import io.deephaven.client.impl.BarrageSessionFactoryBuilder;
import io.deephaven.client.impl.BarrageSubscription;
import io.deephaven.client.impl.ChannelHelper;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.engine.table.Table;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.uri.ApplicationUri;
import io.deephaven.uri.DeephavenTarget;
import io.deephaven.uri.DeephavenUri;
import io.deephaven.uri.FieldUri;
import io.deephaven.uri.QueryScopeUri;
import io.deephaven.uri.RemoteUri;
import io.deephaven.uri.StructuredUri.Visitor;
import io.deephaven.uri.resolver.UriResolver;
import io.deephaven.uri.resolver.UriResolversInstance;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The barrage table resolver is able to resolve {@link RemoteUri remote URIs} into {@link Table tables}.
 *
 * <p>
 * For more advanced use cases, see {@link BarrageSession}.
 *
 * @see RemoteUri remote URI format
 */
@Singleton
public final class BarrageTableResolver implements UriResolver {

    /**
     * The default options, which uses {@link BarrageSubscriptionOptions#useDeephavenNulls()}.
     */
    public static final BarrageSubscriptionOptions OPTIONS = BarrageSubscriptionOptions.builder()
            .useDeephavenNulls(true)
            .build();

    private static final Set<String> SCHEMES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(DeephavenUri.SECURE_SCHEME, DeephavenUri.PLAINTEXT_SCHEME)));

    public static BarrageTableResolver get() {
        return UriResolversInstance.get().find(BarrageTableResolver.class).get();
    }

    private final BarrageSessionFactoryBuilder builder;

    private final ScheduledExecutorService executor;

    private final BufferAllocator allocator;

    private final Map<DeephavenTarget, BarrageSession> sessions;

    @Inject
    public BarrageTableResolver(
            BarrageSessionFactoryBuilder builder, ScheduledExecutorService executor, BufferAllocator allocator) {
        this.builder = Objects.requireNonNull(builder);
        this.executor = Objects.requireNonNull(executor);
        this.allocator = Objects.requireNonNull(allocator);
        this.sessions = new ConcurrentHashMap<>();
    }

    @Override
    public Set<String> schemes() {
        return SCHEMES;
    }

    @Override
    public boolean isResolvable(URI uri) {
        // Note: we are lying right now when we say this supports remote proxied uri - but we are doing that so callers
        // can get a more specific exception with a link to the issue number.
        return RemoteUri.isWellFormed(uri);
    }

    @Override
    public Table resolve(URI uri) throws InterruptedException {
        try {
            return subscribe(RemoteUri.of(uri));
        } catch (TableHandleException e) {
            throw e.asUnchecked();
        }
    }

    /**
     * Create a full-subscription to the remote URI. Uses {@link #OPTIONS}.
     *
     * @param remoteUri the remote URI
     * @return the subscribed table
     */
    public Table subscribe(RemoteUri remoteUri) throws InterruptedException, TableHandleException {
        final DeephavenTarget target = remoteUri.target();
        final TableSpec table = RemoteResolver.of(remoteUri);
        return subscribe(target, table, OPTIONS);
    }

    /**
     * Create a full-subscription to the {@code table} via the {@code targetUri}. Uses {@link #OPTIONS}.
     *
     * @param targetUri the target URI
     * @param table the table spec
     * @return the subscribed table
     */
    public Table subscribe(String targetUri, TableSpec table) throws TableHandleException, InterruptedException {
        return subscribe(DeephavenTarget.of(URI.create(targetUri)), table, OPTIONS);
    }

    /**
     * Create a full-subscription to the {@code table} via the {@code target}.
     *
     * @param target the target
     * @param table the table
     * @param options the options
     * @return the subscribed table
     */
    public Table subscribe(DeephavenTarget target, TableSpec table, BarrageSubscriptionOptions options)
            throws TableHandleException, InterruptedException {
        final BarrageSession session = session(target);
        final BarrageSubscription sub = session.subscribe(table, options);
        return sub.entireTable();
    }

    private BarrageSession session(DeephavenTarget target) {
        // TODO (deephaven-core#1482): BarrageTableResolver cleanup
        return sessions.computeIfAbsent(target, this::newSession);
    }

    private BarrageSession newSession(DeephavenTarget target) {
        return newSession(ChannelHelper.channel(target));
    }

    private BarrageSession newSession(ManagedChannel channel) {
        return builder
                .allocator(allocator)
                .managedChannel(channel)
                .scheduler(executor)
                .build()
                .newBarrageSession();
    }

    static class RemoteResolver implements Visitor {

        public static TableSpec of(RemoteUri remoteUri) {
            return remoteUri.uri().walk(new RemoteResolver(remoteUri.target())).out();
        }

        private final DeephavenTarget target;
        private TableSpec out;

        public RemoteResolver(DeephavenTarget target) {
            this.target = Objects.requireNonNull(target);
        }

        public TableSpec out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(FieldUri fieldUri) {
            out = TicketTable.fromApplicationField(target.host(), fieldUri.fieldName());
        }

        @Override
        public void visit(ApplicationUri applicationField) {
            out = TicketTable.fromApplicationField(applicationField.applicationId(), applicationField.fieldName());
        }

        @Override
        public void visit(QueryScopeUri queryScope) {
            out = TicketTable.fromQueryScopeField(queryScope.variableName());
        }

        @Override
        public void visit(RemoteUri remoteUri) {
            // TODO(deephaven-core#1483): Support resolve URI via gRPC / QST
            throw new UnsupportedOperationException(
                    "Proxying not supported yet, see https://github.com/deephaven/deephaven-core/issues/1483");
        }

        @Override
        public void visit(URI customUri) {
            // TODO(deephaven-core#1483): Support resolve URI via gRPC / QST
            throw new UnsupportedOperationException(
                    "Remote custom URIs not supported yet, see https://github.com/deephaven/deephaven-core/issues/1483");
        }
    }
}
