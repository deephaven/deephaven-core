package io.deephaven.streampublisher;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.streampublisher.KeyOrValueIngestData;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.List;
import java.util.Map;

public interface KeyOrValueSpec {
    public enum KeyOrValue {
        KEY, VALUE
    }

    KeyOrValueIngestData getIngestData(
            KeyOrValue keyOrValue,
            Map<String, ?> configs,
            MutableInt nextColumnIndexMut,
            List<ColumnDefinition<?>> columnDefinitionsOut);

    KeyOrValueProcessor getProcessor(
            TableDefinition tableDef,
            KeyOrValueIngestData data);
}
