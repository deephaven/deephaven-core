package io.deephaven.client.impl;

import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.db.tables.Table;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.uri.ApplicationUri;
import io.deephaven.uri.DeephavenTarget;
import io.deephaven.uri.FieldUri;
import io.deephaven.uri.QueryScopeUri;
import io.deephaven.uri.RemoteUri;
import io.deephaven.uri.ResolvableUri;
import io.deephaven.uri.TableResolver;
import io.deephaven.uri.TableResolversInstance;
import io.deephaven.uri.UriCreator;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;

import javax.inject.Inject;
import javax.inject.Provider;
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
 * The barrage table resolver is able to resolve {@link RemoteUri remote URIs} into tables.
 *
 * <p>
 * For more advanced use cases, see {@link BarrageSession}.
 *
 * @see RemoteUri remote URI format
 */
@Singleton
public final class BarrageTableResolver implements TableResolver {

    /**
     * The default options, which uses {@link BarrageSubscriptionOptions#useDeephavenNulls()}.
     */
    public static final BarrageSubscriptionOptions OPTIONS = BarrageSubscriptionOptions.builder()
            .useDeephavenNulls(true)
            .build();

    private static final Set<String> SCHEMES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(DeephavenTarget.TLS_SCHEME, DeephavenTarget.PLAINTEXT_SCHEME)));

    public static BarrageTableResolver get() {
        return TableResolversInstance.get().find(BarrageTableResolver.class).get();
    }

    // Must be initialized afterwards, since the creator likely contains reference to BarrageTableResolver
    private final Provider<UriCreator> resolver;

    private final BarrageSessionFactoryBuilder builder;

    private final ScheduledExecutorService executor;

    private final BufferAllocator allocator;

    private final Map<DeephavenTarget, BarrageSession> sessions;

    @Inject
    public BarrageTableResolver(Provider<UriCreator> resolver, BarrageSessionFactoryBuilder builder,
            ScheduledExecutorService executor, BufferAllocator allocator) {
        this.resolver = Objects.requireNonNull(resolver);
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
    public Table resolve(URI uri) throws InterruptedException {
        try {
            return resolve(RemoteUri.of(resolver.get(), uri));
        } catch (TableHandleException e) {
            throw e.asUnchecked();
        }
    }

    @Override
    public RemoteUri create(String scheme, String rest) {
        return RemoteUri.of(resolver.get(), URI.create(String.format("%s://%s", scheme, rest)));
    }

    public Table resolve(RemoteUri remoteUri) throws InterruptedException, TableHandleException {
        final DeephavenTarget target = remoteUri.target();
        final TableSpec table = RemoteResolver.of(remoteUri);
        return subscribe(target, table, OPTIONS);
    }

    /**
     * Create a full-subscription to the remote URI. Uses {@link #OPTIONS}.
     *
     * @param remoteUri the remote URI, {@link RemoteUri}.
     * @return the subscribed table
     */
    public Table subscribe(String remoteUri) throws TableHandleException, InterruptedException {
        final RemoteUri uri = RemoteUri.of(resolver.get(), URI.create(remoteUri));
        final DeephavenTarget target = uri.target();
        final TableSpec table = RemoteResolver.of(uri);
        return subscribe(target, table, OPTIONS);
    }

    /**
     * Create a full-subscription to the {@code table}. Uses {@link #OPTIONS}.
     *
     * @param targetUri the {@link DeephavenTarget} URI
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
        // TODO: cleanup sessions after all tables are gone
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

    static class RemoteResolver implements ResolvableUri.Visitor {

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
            throw new UnsupportedOperationException("Proxying not supported yet");
        }

        @Override
        public void visit(URI uri) {
            throw new UnsupportedOperationException("Remote generic URIs not supported yet");
        }
    }
}
