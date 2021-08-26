package io.deephaven.client.examples;

import io.deephaven.qst.table.LabeledTable;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine.ITypeConverter;

import java.nio.file.Paths;

class LabeledTableConverter implements ITypeConverter<LabeledTable> {

    @Override
    public LabeledTable convert(String value) throws Exception {
        int eqIx = value.indexOf('=');
        if (eqIx >= 0) {
            return LabeledTable.of(value.substring(0, eqIx),
                    TableSpec.file(Paths.get(value.substring(eqIx + 1))));
        }
        return LabeledTable.of(value, TableSpec.file(Paths.get(value)));
    }
}
