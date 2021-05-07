package io.deephaven.db.v2.locations;

import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

/**
 * {@link TableLocation} sub-interface for table locations stored in the Apache Parquet columnar format.
 */
public interface ParquetFormatTableLocation<CLT extends ParquetFormatColumnLocation> extends TableLocation<CLT> {

    @Override
    @FinalDefault
    @NotNull
    default Format getFormat() {
        return Format.PARQUET;
    }
}
