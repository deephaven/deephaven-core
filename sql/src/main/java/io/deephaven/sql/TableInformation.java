//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.List;

@Immutable
@SimpleStyle
public abstract class TableInformation {

    public static TableInformation of(List<String> qualifiedName, TableHeader header, TableSpec spec) {
        return ImmutableTableInformation.of(qualifiedName, header, spec);
    }

    @Parameter
    public abstract List<String> qualifiedName();

    @Parameter
    public abstract TableHeader header();

    @Parameter
    public abstract TableSpec spec();
}
