//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.api.ColumnName;
import org.apache.calcite.rel.type.RelDataTypeField;

interface OutputFieldAdapter {

    ColumnName output(RelDataTypeField field);
}
