package io.deephaven.db.tables.remote.preview;

import java.io.Serializable;

/**
 * A Preview Type is used for columns that should be previewed rather than sending all of the data
 * for each value.
 */
public interface PreviewType extends Serializable {
}
