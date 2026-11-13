//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.FieldInfo;
import io.deephaven.client.impl.script.Changes;

/**
 * Renders the variable changes a console reports after executing code.
 */
public final class ChangesFormatter {

    public static String toPrettyString(Changes changes) {
        final StringBuilder sb = new StringBuilder();
        if (changes.errorMessage().isPresent()) {
            sb.append("Error: ").append(changes.errorMessage().get()).append(System.lineSeparator());
        }
        if (changes.isEmpty()) {
            sb.append("No displayable variables updated").append(System.lineSeparator());
        } else {
            for (FieldInfo fieldInfo : changes.changes().created()) {
                sb.append(fieldInfo.type().orElse("?")).append(' ').append(fieldInfo.name()).append(" = <new>")
                        .append(System.lineSeparator());
            }
            for (FieldInfo fieldInfo : changes.changes().updated()) {
                sb.append(fieldInfo.type().orElse("?")).append(' ').append(fieldInfo.name())
                        .append(" = <updated>")
                        .append(System.lineSeparator());
            }
            for (FieldInfo fieldInfo : changes.changes().removed()) {
                sb.append(fieldInfo.type().orElse("?")).append(' ').append(fieldInfo.name()).append(" <removed>")
                        .append(System.lineSeparator());
            }
        }
        return sb.toString();
    }

    private ChangesFormatter() {}
}
