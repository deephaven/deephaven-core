/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.sql;

import io.deephaven.api.literal.Literal;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

final class LiteralAdapter {
    public static Literal of(RexLiteral literal) {
        SqlTypeName typeName = literal.getTypeName();
        if (typeName == SqlTypeName.DECIMAL) {
            // get the "inner" type, if decimal.
            // for example, calcite treats
            // "SELECT CAST(1 as BIGINT)"
            // as a DECIMAL 1:BIGINT
            typeName = literal.getType().getSqlTypeName();
        }
        switch (typeName) {
            case BOOLEAN:
                return RexLiteral.booleanValue(literal) ? Literal.of(true) : Literal.of(false);
            case BIGINT:
                return Literal.of(literal.getValueAs(Long.class));
            case INTEGER:
                return Literal.of(literal.getValueAs(Integer.class));
        }
        throw new UnsupportedOperationException("Literal support for " + typeName + " " + literal);
    }
}
