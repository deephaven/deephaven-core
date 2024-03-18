//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.preview;

import java.io.Serializable;

/**
 * A Preview Type is used for columns that should be previewed rather than sending all of the data for each value.
 */
public interface PreviewType extends Serializable {
}
