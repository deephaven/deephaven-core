package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.client.impl.ExportRequest.Listener;
import io.deephaven.qst.table.TableSpec;
import org.immutables.value.Value.Immutable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * The exports request structure. Is a list of {@link ExportRequest export requests}.
 *
 * @see Session#export(ExportsRequest)
 */
@Immutable
@BuildableStyle
public abstract class ExportsRequest implements Iterable<ExportRequest> {

    public interface Builder {

        Builder addRequests(ExportRequest element);

        Builder addRequests(ExportRequest... elements);

        Builder addAllRequests(Iterable<? extends ExportRequest> elements);

        ExportsRequest build();
    }

    public static Builder builder() {
        return ImmutableExportsRequest.builder();
    }

    public static ExportsRequest of(ExportRequest... requests) {
        return builder().addRequests(requests).build();
    }

    public static ExportsRequest of(Collection<ExportRequest> requests) {
        return builder().addAllRequests(requests).build();
    }

    /**
     * A convenience method for creating an export requests with a shared {@link Listener#logging() logging listener}.
     *
     * @param tables the tables
     * @return the requests
     */
    public static ExportsRequest logging(TableSpec... tables) {
        return logging(Arrays.asList(tables));
    }

    /**
     * A convenience method for creating an export requests with a shared {@link Listener#logging() logging listener}.
     *
     * @param tables the tables
     * @return the requests
     */
    public static ExportsRequest logging(Iterable<TableSpec> tables) {
        Listener listener = Listener.logging();
        Builder builder = builder();
        for (TableSpec table : tables) {
            builder.addRequests(ExportRequest.of(table, listener));
        }
        return builder.build();
    }

    // note: it's valid to use the same table multiple times in a single request
    abstract List<ExportRequest> requests();

    public final Iterable<TableSpec> tables() {
        return () -> requests().stream().map(ExportRequest::table).iterator();
    }

    public final int size() {
        return requests().size();
    }

    @Override
    public final Iterator<ExportRequest> iterator() {
        return requests().iterator();
    }

    @Override
    public final void forEach(Consumer<? super ExportRequest> action) {
        requests().forEach(action);
    }

    @Override
    public final Spliterator<ExportRequest> spliterator() {
        return requests().spliterator();
    }
}
