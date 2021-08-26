package io.deephaven.client.examples;

import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine.ITypeConverter;

import java.nio.file.Paths;

class TableConverter implements ITypeConverter<TableSpec> {

    @Override
    public TableSpec convert(String value) throws Exception {
        return TableSpec.file(Paths.get(value));
    }
}
