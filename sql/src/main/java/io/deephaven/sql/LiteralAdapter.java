//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
            case TINYINT:
                return Literal.of(literal.getValueAs(Byte.class));
            case SMALLINT:
                return Literal.of(literal.getValueAs(Short.class));
            case INTEGER:
                return Literal.of(literal.getValueAs(Integer.class));
            case BIGINT:
                return Literal.of(literal.getValueAs(Long.class));
            case REAL:
                return Literal.of(literal.getValueAs(Float.class));
            case DOUBLE:
            case FLOAT:
                return Literal.of(literal.getValueAs(Double.class));
            case CHAR:
            case VARCHAR:
                return Literal.of(literal.getValueAs(String.class));
        }
        throw new UnsupportedOperationException("Literal support for " + typeName + " " + literal);
    }
}
