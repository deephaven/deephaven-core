package io.deephaven.streampublisher.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.streampublisher.KeyOrValueIngestData;
import io.deephaven.streampublisher.KeyOrValueProcessor;
import io.deephaven.streampublisher.KeyOrValueSpec;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;

/**
 * JSON spec.
 */
public class JsonConsumeImpl implements KeyOrValueSpec {
    @Nullable
    private final ObjectMapper objectMapper;
    private final ColumnDefinition<?>[] columnDefinitions;
    private final Map<String, String> fieldToColumnName;

    public JsonConsumeImpl(
            @NotNull final ColumnDefinition<?>[] columnDefinitions,
            @Nullable final Map<String, String> fieldNameToColumnName,
            @Nullable final ObjectMapper objectMapper) {
        this.columnDefinitions = columnDefinitions;
        this.fieldToColumnName = mapNonPointers(fieldNameToColumnName);
        this.objectMapper = objectMapper;
    }

    @Override
    public KeyOrValueIngestData getIngestData(KeyOrValueSpec.KeyOrValue keyOrValue,
            Map<String, ?> configs, MutableInt nextColumnIndexMut,
            List<ColumnDefinition<?>> columnDefinitionsOut) {
        final KeyOrValueIngestData data = new KeyOrValueIngestData();
        data.toObjectChunkMapper = jsonToObjectChunkMapper(objectMapper);
        columnDefinitionsOut.addAll(Arrays.asList(columnDefinitions));
        // Populate out field to column name mapping from two potential sources.
        data.fieldPathToColumnName = new HashMap<>(columnDefinitions.length);
        final Set<String> coveredColumns = new HashSet<>(columnDefinitions.length);
        if (fieldToColumnName != null) {
            for (final Map.Entry<String, String> entry : fieldToColumnName.entrySet()) {
                final String colName = entry.getValue();
                data.fieldPathToColumnName.put(entry.getKey(), colName);
                coveredColumns.add(colName);
            }
        }
        for (final ColumnDefinition<?> colDef : columnDefinitions) {
            final String colName = colDef.getName();
            if (!coveredColumns.contains(colName)) {
                final String jsonPtrStr =
                        JsonConsumeImpl.mapFieldNameToJsonPointerStr(colName);
                data.fieldPathToColumnName.put(jsonPtrStr, colName);
            }
        }
        return data;
    }

    @Override
    public KeyOrValueProcessor getProcessor(TableDefinition tableDef, KeyOrValueIngestData data) {
        return JsonNodeChunkAdapter.make(
                tableDef,
                ci -> StreamChunkUtils.chunkTypeForColumnIndex(tableDef, ci),
                data.fieldPathToColumnName,
                true);
    }

    private static Map<String, String> mapNonPointers(final Map<String, String> fieldNameToColumnName) {
        if (fieldNameToColumnName == null) {
            return null;
        }
        final boolean needsMapping =
                fieldNameToColumnName.keySet().stream().anyMatch(key -> !key.startsWith("/"));
        if (!needsMapping) {
            return fieldNameToColumnName;
        }
        final Map<String, String> ans = new HashMap<>(fieldNameToColumnName.size());
        for (Map.Entry<String, String> entry : fieldNameToColumnName.entrySet()) {
            final String key = entry.getKey();
            if (key.startsWith("/")) {
                ans.put(key, entry.getValue());
            } else {
                ans.put(mapFieldNameToJsonPointerStr(key), entry.getValue());
            }
        }
        return ans;
    }

    /***
     * JSON field names (or "key") can be any string, so in principle they can contain the '/' character. JSON Pointers
     * assign special meaning to the '/' character, so actual '/' in the key they need to be encoded differently. The
     * spec for JSON Pointers (see RFC 6901) tells us to encode '/' using "~1". If we need the '~' character we have to
     * encode that as "~0". This method does this simple JSON Pointer encoding.
     *
     * @param key an arbitrary JSON field name, that can potentially contain the '/' or '~' characters.
     * @return a JSON Pointer encoded as a string for the provided key.
     */
    public static String mapFieldNameToJsonPointerStr(final String key) {
        return "/" + key.replace("~", "~0").replace("/", "~1");
    }

    private static Function<Object, Object> jsonToObjectChunkMapper(@Nullable final ObjectMapper mapper) {
        return (final Object in) -> {
            final String json;
            try {
                json = (String) in;
            } catch (ClassCastException ex) {
                throw new UncheckedDeephavenException("Could not convert input to json string", ex);
            }
            return JsonNodeUtil.makeJsonNode(mapper, json);
        };
    }
}
