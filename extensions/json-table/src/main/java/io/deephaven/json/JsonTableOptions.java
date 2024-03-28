//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

@Immutable
@BuildableStyle
public abstract class JsonTableOptions {
    private static final Function<List<String>, String> TO_COLUMN_NAME = JsonTableOptions::toColumnName;

    public static Builder builder() {
        return ImmutableJsonTableOptions.builder();
    }

    public static String toColumnName(List<String> path) {
        return path.isEmpty() ? "Value" : String.join("_", path);
    }

    public abstract ValueOptions options();

    public abstract List<Source> sources();

    @Default
    public boolean multiValueSupport() {
        return false;
    }

    @Default
    public int chunkSize() {
        return 1024;
    }

    /**
     * The maximum number of threads to use. Explicitly settings this value to {@code 1} effectively disables concurrent
     * processing and ensures the {@link #sources()} will be processed in-order. The implementation will use the minimum
     * of this value and the size of {@link #sources()} to set the number of threads. Defaults to
     * {@code Runtime.getRuntime().availableProcessors()}.
     *
     * @see Runtime#availableProcessors()
     */
    @Default
    public int maxThreads() {
        return Runtime.getRuntime().availableProcessors();
    }

    /**
     * The name. By default, is {@code UUID.randomUUID().toString()}.
     *
     * @return the name
     */
    @Default
    public String name() {
        return UUID.randomUUID().toString();
    }

    /**
     * Equivalent to {@code ExecutionContext.getContext().getUpdateGraph()}.
     *
     * @return
     */
    @Default
    public UpdateSourceRegistrar updateSourceRegistrar() {
        return ExecutionContext.getContext().getUpdateGraph();
    }

    /**
     * Equivalent to {@link JsonTableOptions#toColumnName(List)}.
     *
     * @return the naming function
     */
    @Default
    public Function<List<String>, String> namingFunction() {
        return TO_COLUMN_NAME;
    }

    /**
     * The extra attributes.
     *
     * @return the extra attributes
     */
    public abstract Map<String, Object> extraAttributes();

    /**
     * Creates a blink {@link Table} from the combined rows from {@link #sources()}. While the intra-source rows are
     * guaranteed to be in relative order with each other, no guarantee is made about inter-source rows. That is, rows
     * from different sources may be interspersed with each other, and there is no guarantee about the order in which
     * the sources are processed. The one exception to this is when {@link #maxThreads()} is {@code 1}, in which case
     * the sources are guaranteed to be processed in-order.
     * 
     * @return the blink table
     */
    public final Table execute() {
        return JsonPublishingProvider.serviceLoader().of(this);
    }

    // todo: potential for partitioned table in future based on sources
    // todo: potential for multiple outputs based on TypedObjectOptions

    public interface Builder {
        Builder options(ValueOptions options);

        Builder addSources(Source element);

        Builder addSources(Source... elements);

        Builder addAllSources(Iterable<? extends Source> elements);

        default Builder addSources(String content) {
            return addSources(Source.of(content));
        }

        default Builder addSources(ByteBuffer buffer) {
            return addSources(Source.of(buffer));
        }

        default Builder addSources(CharBuffer buffer) {
            return addSources(Source.of(buffer));
        }

        default Builder addSources(File file) {
            return addSources(Source.of(file));
        }

        default Builder addSources(Path path) {
            return addSources(Source.of(path));
        }

        default Builder addSources(InputStream inputStream) {
            return addSources(Source.of(inputStream));
        }

        default Builder addSources(URL url) {
            return addSources(Source.of(url));
        }

        Builder name(String name);

        Builder updateSourceRegistrar(UpdateSourceRegistrar updateSourceRegistrar);

        Builder multiValueSupport(boolean multiValueSupport);

        Builder maxThreads(int maxThreads);

        Builder chunkSize(int chunkSize);

        Builder namingFunction(Function<List<String>, String> namingFunction);

        Builder putExtraAttributes(String key, Object value);

        Builder putAllExtraAttributes(Map<String, ? extends Object> entries);

        JsonTableOptions build();
    }

    @Check
    final void checkSources() {
        if (sources().isEmpty()) {
            throw new IllegalArgumentException("sources must be non-empty");
        }
    }

    @Check
    final void checkNumThreads() {
        if (maxThreads() <= 0) {
            throw new IllegalArgumentException("numThreads must be positive");
        }
    }
}
