//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.util.annotations.InternalUseOnly;
import org.immutables.value.Value;
import org.immutables.value.Value.Default;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class provides instructions intended for writing Iceberg tables. The default values documented in this class may
 * change in the future. As such, callers may wish to explicitly set the values.
 */
public abstract class IcebergWriteInstructions implements IcebergBaseInstructions {
    // @formatter:off
    /**
     * Specifies whether to verify that the partition spec and schema of the table being written are consistent with the
     * Iceberg table.
     *
     * <p>Verification behavior differs based on the operation type:</p>
     * <ul>
     *   <li><strong>Appending Data or Writing Data Files:</strong> Verification is enabled by default. It ensures that:
     *     <ul>
     *       <li>All columns from the Deephaven table are present in the Iceberg table and have compatible types.</li>
     *       <li>All required columns in the Iceberg table are present in the Deephaven table.</li>
     *       <li>The set of partitioning columns in both the Iceberg and Deephaven tables are identical.</li>
     *     </ul>
     *   </li>
     *   <li><strong>Overwriting Data:</strong> Verification is disabled by default. When enabled, it ensures that the
     *   schema and partition spec of the table being written are identical to those of the Iceberg table.</li>
     * </ul>
     */
    public abstract Optional<Boolean> verifySchema();
    // @formatter:on

    /**
     * A one-to-one {@link Map map} from Deephaven to Iceberg column names to use when writing deephaven tables to
     * Iceberg tables.
     */
    // TODO Please suggest better name for this method, on the read side its just called columnRenames
    public abstract Map<String, String> dhToIcebergColumnRenames();

    /**
     * The inverse map of {@link #dhToIcebergColumnRenames()}.
     */
    @InternalUseOnly
    @Value.Lazy
    public Map<String, String> icebergToDhColumnRenames() {
        return dhToIcebergColumnRenames().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    public interface Builder<INSTRUCTIONS_BUILDER> extends IcebergBaseInstructions.Builder<INSTRUCTIONS_BUILDER> {
        INSTRUCTIONS_BUILDER verifySchema(boolean verifySchema);

        INSTRUCTIONS_BUILDER putDhToIcebergColumnRenames(String key, String value);

        INSTRUCTIONS_BUILDER putAllDhToIcebergColumnRenames(Map<String, ? extends String> entries);
    }

    @Value.Check
    final void checkColumnRenamesUnique() {
        if (dhToIcebergColumnRenames().size() != dhToIcebergColumnRenames().values().stream().distinct().count()) {
            throw new IllegalArgumentException("Duplicate values in column renames, values must be unique");
        }
    }
}
